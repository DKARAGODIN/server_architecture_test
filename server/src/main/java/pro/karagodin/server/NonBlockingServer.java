package pro.karagodin.server;

import pro.karagodin.message.Data;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NonBlockingServer implements Runnable {

	private static final Logger log = Logger.getLogger(NonBlockingServer.class.getName());

	private final Map<SocketAddress, Client> clients = new ConcurrentHashMap<>();
	private final Map<Client, Integer> clientRequestsCounter = new ConcurrentHashMap<>();

	private final Map<Client, Long> startTimes = new ConcurrentHashMap<>();
	private final Map<Client, List<Long>> sortTimes = new ConcurrentHashMap<>();
	private final ArrayBlockingQueue<Message> messagesForClients = new ArrayBlockingQueue<>(1000);
	private final ArrayBlockingQueue<CloseMessage> closeMessages = new ArrayBlockingQueue<>(1000);

	private final List<Long> serverTimes = Collections.synchronizedList(new ArrayList<>());
	private final ByteBuffer buffer = ByteBuffer.allocate(1024);
	private final List<ByteBuffer> parts = new ArrayList<>();
	private int clientId;

	private final ExecutorService workers = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	@Override
	public void run() {
		startResetListener();
		Runnable closerConnectionTask = () -> {
			Map<Client, Integer> closedMessages = new HashMap<>();
			Map<Client, Integer> finalId = new HashMap<>();
			while (true) {
				try {
					CloseMessage message = closeMessages.take();
					boolean timeToClose = false;
					if (message.closed) {
						finalId.put(message.client, message.requestId);
						Integer lastClosedMessage = closedMessages.get(message.client);
						if (lastClosedMessage != null) {
							if (lastClosedMessage <= message.requestId) {
								timeToClose = true;
							}
						}
					} else {
						closedMessages.put(message.client, message.requestId);
						Integer fin = finalId.get(message.client);
						if (fin != null) {
							if (message.requestId <= fin) {
								timeToClose = true;
							}
						}
					}
					if (timeToClose) {
						log.info("Closing channel for client " + clientId);
						message.client.channel().close();
						serverTimes.add(System.currentTimeMillis() - startTimes.get(message.client));
						startTimes.remove(message.client);
					}
				} catch (Exception ex) {
					log.log(Level.SEVERE, ex.getMessage(), ex);
				}
			}
		};
		Thread closerThread = new Thread(closerConnectionTask);
		closerThread.setDaemon(true);
		closerThread.start();

		Runnable senderTask = () -> {
			while (true) {
				try {
					sendMessagesToClients();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		};
		Thread senderThread = new Thread(senderTask);
		senderThread.setDaemon(true);
		senderThread.start();

		try {
			try (var serverSocketChannel = ServerSocketChannel.open()) {
				serverSocketChannel.configureBlocking(false);
				var serverSocket = serverSocketChannel.socket();
				serverSocket.bind(new InetSocketAddress("localhost", Main.SERVER_PORT));
				try (var selector = Selector.open()) {
					serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
					log.info(String.format("Server started. addr:%s, port:%s", "localhost", Main.SERVER_PORT));
					while (true) {
						try {
							selector.select(this::performIO);
						} catch (CommunicationException ex) {
							SocketAddress clientAddress = getSocketAddress(ex.getSocketChannel());
							log.severe(String.format("error in client communication: %s, message %s", clientAddress, ex));
							disconnect(clientAddress);
						} catch (Exception ex) {
							log.log(Level.SEVERE, ex.getMessage(), ex);
						}
					}
				}
			}
		} catch (Exception ex) {
			log.severe(ex.getMessage());
		}
	}

	private void performIO(SelectionKey selectedKey) {
		if (selectedKey.isAcceptable()) {
			acceptConnection(selectedKey);
		} else if (selectedKey.isReadable()) {
			readFromClient(selectedKey);
		}
	}

	private void acceptConnection(SelectionKey key) {
		clientId++;
		log.info("Accepted new client with id " + clientId);
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
		try {
			var clientSocketChannel = serverSocketChannel.accept();
			var selector = key.selector();

			clientSocketChannel.configureBlocking(false);
			clientSocketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

			SocketAddress remoteAddress = clientSocketChannel.getRemoteAddress();
			Client client = new Client(clientId, clientSocketChannel);
			clients.put(remoteAddress, client);
			startTimes.put(client, System.currentTimeMillis());
			sortTimes.put(client, new ArrayList<>());
		} catch (Exception ex) {
			log.severe(String.format("can't accept new client on:%s", key));
		}
	}

	private void readFromClient(SelectionKey selectionKey) {
		SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
		Client client;
		Integer requestNumber;
		try {
			client = clients.get(socketChannel.getRemoteAddress());
			requestNumber = clientRequestsCounter.merge(client, 1, Integer::sum);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		log.info(String.format("Start reading %s request from client %s", requestNumber, client.id));
		Data data = readRequest(socketChannel);
		log.info(String.format("Finished reading %s request from client %s", requestNumber, client.id));

		if (data == null) {
			closeMessages.add(new CloseMessage(client, requestNumber, true));
			log.info(String.format("No more data from from client %s", clientId));
		} else {
			Runnable runnable = () -> {
				log.info(String.format("Start processing %s request from client %s", requestNumber, client.id));
				long startSort = System.currentTimeMillis();
				Data responseData = Sorter.bubleSort(data);
				sortTimes.get(client).add(System.currentTimeMillis() - startSort);
				log.info(String.format("Finished processing %s request from client %s", requestNumber, client.id));
				messagesForClients.add(new Message(getSocketAddress(socketChannel), responseData, requestNumber));
			};
			workers.submit(runnable);
		}
	}

	private Data readRequest(SocketChannel socketChannel) {
		try {
			int usedIdx = 0;
			int readBytesTotal = 0;
			int readBytes;
			while ((readBytes = socketChannel.read(buffer)) > 0) {
				buffer.flip();
				if (usedIdx >= parts.size()) {
					parts.add(ByteBuffer.allocateDirect(readBytes));
				}

				if (parts.get(usedIdx).capacity() < readBytes) {
					parts.add(usedIdx, ByteBuffer.allocateDirect(readBytes));
				}

				parts.get(usedIdx).put(buffer);
				buffer.flip();
				readBytesTotal += readBytes;
				usedIdx++;
			}

			if (readBytesTotal == 0) {
				return null;
			}
			var result = new byte[readBytesTotal];
			var resultIdx = 0;

			for (var idx = 0; idx < usedIdx; idx++) {
				var part = parts.get(idx);
				part.flip();
				part.get(result, resultIdx, part.limit());
				resultIdx += part.limit();
				part.flip();
			}
			parts.clear();

			try (ByteArrayInputStream in = new ByteArrayInputStream(result)) {
				return Data.parseDelimitedFrom(in);
			}
		} catch (Exception ex) {
			throw new RuntimeException("Reading error", ex);
		}
	}

	private SocketAddress getSocketAddress(SocketChannel socketChannel) {
		try {
			return socketChannel.getRemoteAddress();
		} catch (Exception ex) {
			throw new RuntimeException("get RemoteAddress error", ex);
		}
	}


	private void disconnect(SocketAddress clientAddress) {
		var client = clients.remove(clientAddress);
		if (client != null) {
			try {
				client.channel().close();
			} catch (IOException e) {
				log.severe(String.format("clientChannel:%s, closing error:%s", clientAddress, e.getMessage()));
			}
		}
	}

	private void sendMessagesToClients() throws InterruptedException {
		Message msg;
		while (true) {
			msg = messagesForClients.take();
			Client client = clients.get(msg.address());
			if (client == null) {
				log.severe(String.format("Client %s not found", msg.address()));
			} else {
				try {
					log.info(String.format("Start sending %s response to client %s", msg.requestId, client.id));
					ByteArrayOutputStream bos = new ByteArrayOutputStream();
					msg.data().writeDelimitedTo(bos);
					client.channel().write(ByteBuffer.wrap(bos.toByteArray()));
					log.info(String.format("Finished sending %s response to client %s", msg.requestId, client.id));
					closeMessages.add(new CloseMessage(client, msg.requestId, false));
				} catch (Exception ex) {
					throw new CommunicationException("Write to the client error", ex, client.channel());
				}
			}
		}
	}

	private void startResetListener() {
		Runnable resetStatsServer = () -> {
			while(true) {
				try (ServerSocket serverSocket = new ServerSocket(Main.SERVER_RESET_PORT);
					Socket socket = serverSocket.accept()) {
					log.info("Received reset call");

					double avgServerTime = serverTimes.stream().mapToLong(val -> val).average().orElse(0.0);
					double avgSortTime =  sortTimes.values().stream().flatMap(Collection::stream).mapToDouble(val -> val).average().orElse(0.0);
					String line = String.format("Average server time per client - %f. Average sort time - %f", avgServerTime, avgSortTime);
					try (BufferedWriter writer = new BufferedWriter(new FileWriter("non_blocking_server_result", true))) {
						writer.append(line);
						writer.newLine();
					}

					serverTimes.clear();
					for (Client client : clients.values()) {
						try {
							client.channel().close();
						} catch (IOException e) {
							//Ignore
						}
					}
					clients.clear();
					clientRequestsCounter.clear();
					messagesForClients.clear();
					startTimes.clear();
					sortTimes.clear();
					log.info("Reset finished");
					try (BufferedWriter writer = new BufferedWriter(new FileWriter("non_blocking_server_result", true))) {
						writer.newLine();
					}
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
		};
		Thread resetThread = new Thread(resetStatsServer);
		resetThread.setDaemon(true);
		resetThread.setName("Reset_Listener");
		resetThread.start();

	}


	private record Message(SocketAddress address, Data data, Integer requestId) {}

	private record Client(Integer id, SocketChannel channel) {}

	private record CloseMessage(Client client, Integer requestId, boolean closed) {}

	private static class CommunicationException extends RuntimeException {

		private final SocketChannel socketChannel;

		public CommunicationException(String message, Throwable cause, SocketChannel socketChannel) {
			super(message, cause);
			this.socketChannel = socketChannel;
		}

		public SocketChannel getSocketChannel() {
			return socketChannel;
		}
	}
}
