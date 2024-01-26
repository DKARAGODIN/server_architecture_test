package pro.karagodin.server;

import pro.karagodin.message.Data;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channels;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static pro.karagodin.server.Main.SERVER_PORT;

public class AsynchServer implements Runnable {

	private static final Logger log = Logger.getLogger(AsynchServer.class.getName());

	private final Map<SocketAddress, Client> clients = new ConcurrentHashMap<>();
	private final ArrayBlockingQueue<Message> messagesForClients = new ArrayBlockingQueue<>(1000);
	private final AtomicInteger clientsCounter = new AtomicInteger(0);

	private final List<Long> serverTimes = Collections.synchronizedList(new ArrayList<>());
	private final Map<Client, List<Long>> sortTimes = new ConcurrentHashMap<>();

	private final ExecutorService workers = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	@Override
	public void run() {
		startResetListener();
		Runnable senderTask = () -> {
			sendMessagesToClients();
		};
		Thread senderThread = new Thread(senderTask);
		senderThread.setDaemon(true);
		senderThread.start();

		try (AsynchronousServerSocketChannel serverChannel = AsynchronousServerSocketChannel.open()) {
			if (serverChannel.isOpen()) {
				serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024);
				serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
				serverChannel.bind(new InetSocketAddress("localhost", SERVER_PORT));

				log.info(String.format("Server started. addr:%s, port:%s", "localhost", Main.SERVER_PORT));
				while (true) {
					serverChannel.accept(null, new CompletionHandler<>() {

						@Override
						public void completed(AsynchronousSocketChannel clientChannel, Object attachment) {
							if (serverChannel.isOpen()){
								serverChannel.accept(null, this);
							}
							int clientId = clientsCounter.incrementAndGet();
							log.info("Accepted new client with id " + clientId);
							try {
								Client client =  new Client(clientId, clientChannel);
								SocketAddress clientAddress = clientChannel.getRemoteAddress();
								clients.put(clientAddress, client);
								sortTimes.put(client, new ArrayList<>());
								InputStream is = Channels.newInputStream(clientChannel);
								int requestNumber = 0;
								long startTime = System.currentTimeMillis();
								while (true) {
									requestNumber++;
									log.info(String.format("Start reading %s request from client %s", requestNumber, client.id));
									Data receivedData = Data.parseDelimitedFrom(is);
									log.info(String.format("Finished reading %s request from client %s", requestNumber, client.id));
									if (receivedData == null) {
										log.info(String.format("No more data from from client %s", client.id));
										disconnect(clientAddress);
										break;
									} else {
										int finalRequestNumber = requestNumber;
										Runnable runnable = () -> {
											log.info(String.format("Start processing %s request from client %s", finalRequestNumber, client.id));
											long startSort = System.currentTimeMillis();
											Data responseData = Sorter.bubleSort(receivedData);
											sortTimes.get(client).add(System.currentTimeMillis() - startSort);
											log.info(String.format("Finished processing %s request from client %s", finalRequestNumber, client.id));
											messagesForClients.add(new Message(getSocketAddress(clientChannel), responseData, finalRequestNumber));
										};
										workers.submit(runnable);
									}
								}
								serverTimes.add(System.currentTimeMillis() - startTime);
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						}
						@Override
						public void failed(Throwable exc, Object attachment) {
							serverChannel.accept(null, this);
							throw new UnsupportedOperationException("Cannot accept connections!");
						}

						private SocketAddress getSocketAddress(AsynchronousSocketChannel socketChannel) {
							try {
								return socketChannel.getRemoteAddress();
							} catch (Exception ex) {
								throw new RuntimeException("get RemoteAddress error", ex);
							}
						}

						private void disconnect(SocketAddress clientAddress) {
							Client client = clients.remove(clientAddress);
							log.info("Closing client with id " + client.id);
							if (client != null) {
								try {
									client.channel.close();
								} catch (IOException e) {
									log.severe(String.format("clientChannel:%s, closing error:%s", e.getMessage()));
								}
							}
						}
					});
					System.in.read();
				}
			}
		} catch (IOException ex) {
			log.severe(ex.getMessage());
		}
	}

	private void sendMessagesToClients() {
		Message msg;
		while (true) {
			try {
				msg = messagesForClients.take();
				Client client = clients.get(msg.address());
				if (client == null) {
					log.severe(String.format("Client %s not found", msg.address()));
				} else {
					log.info(String.format("Start sending %s response to client %s", msg.requestId, client.id));
					ByteArrayOutputStream bos = new ByteArrayOutputStream();
					msg.data().writeDelimitedTo(bos);
					client.channel().write(ByteBuffer.wrap(bos.toByteArray()));
					log.info(String.format("Finished sending %s response to client %s", msg.requestId, client.id));
				}
			} catch (Exception e) {
				log.severe(e.getMessage());
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
					double avgSortTime = sortTimes.values().stream().flatMap(Collection::stream).mapToDouble(val -> val).average().orElse(0.0);
					String line = String.format("Average server time per client - %f. Average sort time - %f", avgServerTime, avgSortTime);
					try (BufferedWriter writer = new BufferedWriter(new FileWriter("asynch_server_result", true))) {
						writer.append(line);
						writer.newLine();
					}
					serverTimes.clear();
					sortTimes.clear();
					log.info("Reset finished");
					try (BufferedWriter writer = new BufferedWriter(new FileWriter("asynch_server_result", true))) {
						writer.newLine();
					}
				} catch (Throwable e) {
					log.severe(e.getMessage());
				}
			}
		};
		Thread resetThread = new Thread(resetStatsServer);
		resetThread.setDaemon(true);
		resetThread.setName("Reset_Listener");
		resetThread.start();

	}



	private record Message(SocketAddress address, Data data, Integer requestId) {}

	private record Client(Integer id, AsynchronousSocketChannel channel) {}

}

