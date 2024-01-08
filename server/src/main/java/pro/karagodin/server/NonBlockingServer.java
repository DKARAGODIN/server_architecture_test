package pro.karagodin.server;

import pro.karagodin.message.Data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class NonBlockingServer implements Runnable {


	//https://github.com/petrelevich/tcp-server-client/blob/main/tcp-server/src/main/java/ru/tcp/Server.java
	private static final Logger log = Logger.getLogger(BlockingServer.class.getName());

	private final Map<SocketAddress, SocketChannel> clients = new HashMap<>();
	private final Queue<SocketAddress> connectedClientsEvents = new ConcurrentLinkedQueue<>();
	private final Queue<SocketAddress> disConnectedClientsEvents = new ConcurrentLinkedQueue<>();
	private final Queue<Message> messagesForClients = new ArrayBlockingQueue<>(1000);
	private final Queue<Message> messagesFromClients = new ArrayBlockingQueue<>(1000);
	private long messagesFromClientsCounter;

	private final ByteBuffer buffer = ByteBuffer.allocate(1024);
	private final List<ByteBuffer> parts = new ArrayList<>();

	@Override
	public void run() {
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
							sendMessagesToClients();
						} catch (CommunicationException ex) {
							//SocketChannel clientAddress = getSocketAddress(ex.getSocketChannel());
							//log.severe(String.format("error in client communication: %s, message %s", clientAddress, ex));
							//disconnect(clientAddress);
						} catch (Exception ex) {
							log.severe(String.format("unexpected error:{}", ex.getMessage()));
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
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
		try {
			var clientSocketChannel = serverSocketChannel.accept();
			var selector = key.selector();

			log.info(String.format("accept client connection, key:%s, selector:%s, clientSocketChannel:%s",
					key,
					selector,
					clientSocketChannel));

			clientSocketChannel.configureBlocking(false);
			clientSocketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

			SocketAddress remoteAddress = clientSocketChannel.getRemoteAddress();
			clients.put(remoteAddress, clientSocketChannel);
			connectedClientsEvents.add(remoteAddress);
		} catch (Exception ex) {
			log.severe(String.format("can't accept new client on:%s", key));
		}
	}

	private void readFromClient(SelectionKey selectionKey) {
		SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
		log.info(String.format("%s. read from client", socketChannel));

		Data data = readRequest(socketChannel);
		if (data == null) {
			disconnect(getSocketAddress(socketChannel));
		} else {

			messagesFromClientsCounter++;
			messagesFromClients.add(new Message(getSocketAddress(socketChannel), data));
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
			throw new CommunicationException("Reading error", ex, socketChannel);
		}
	}

	private SocketAddress getSocketAddress(SocketChannel socketChannel) {
		try {
			return socketChannel.getRemoteAddress();
		} catch (Exception ex) {
			throw new CommunicationException("get RemoteAddress error", ex, socketChannel);
		}
	}


	private void disconnect(SocketAddress clientAddress) {
		var clientChannel = clients.remove(clientAddress);
		if (clientChannel != null) {
			try {
				clientChannel.close();
			} catch (IOException e) {
				log.severe(String.format("clientChannel:%s, closing error:%s", clientAddress, e.getMessage()));
			}
		}
		disConnectedClientsEvents.add(clientAddress);
	}

	private void sendMessagesToClients() {
		Message msg;
		while ((msg = messagesFromClients.poll()) != null) {
			var client = clients.get(msg.address());
			if (client == null) {
//				logger.error("client {} not found", msg.clientAddress());
			} else {
				write(client, msg.data());
			}
		}
	}

	private void write(SocketChannel clientChannel, Data data) {
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			data.writeDelimitedTo(bos);
			clientChannel.write(ByteBuffer.wrap(bos.toByteArray()));
		} catch (Exception ex) {
			throw new CommunicationException("Write to the client error", ex, clientChannel);
		}
	}

	private record Message(SocketAddress address, Data data) {}
}
