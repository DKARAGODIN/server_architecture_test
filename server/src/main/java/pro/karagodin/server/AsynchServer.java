package pro.karagodin.server;

import pro.karagodin.message.Data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import static pro.karagodin.server.Main.SERVER_PORT;

public class AsynchServer implements Runnable {

	private static final Logger log = Logger.getLogger(AsynchServer.class.getName());

	private final Map<SocketAddress, AsynchronousSocketChannel> clients = new HashMap<>();
	private final Queue<SocketAddress> connectedClientsEvents = new ConcurrentLinkedQueue<>();
	private final Queue<SocketAddress> disConnectedClientsEvents = new ConcurrentLinkedQueue<>();
	private final Queue<Message> messagesForClients = new ArrayBlockingQueue<>(1000);
	private final Queue<Message> messagesFromClients = new ArrayBlockingQueue<>(1000);

	final ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
	private final List<ByteBuffer> parts = new ArrayList<>();


	@Override
	public void run() {
		try (AsynchronousServerSocketChannel serverChannel = AsynchronousServerSocketChannel.open()) {
			if (serverChannel.isOpen()) {
				serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024);
				serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
				serverChannel.bind(new InetSocketAddress("localhost", SERVER_PORT));
				while (true) {
					serverChannel.accept(null, new CompletionHandler<>() {

						@Override
						public void completed(AsynchronousSocketChannel result, Object attachment) {
							if (serverChannel.isOpen()){
								serverChannel.accept(null, this);
							}
							try {
								clients.put(result.getRemoteAddress(),result);
							} catch (IOException e) {
								throw new RuntimeException(e);
							}

							while(true) {
								Data data = readRequest(result);
								if (data != null) {
									try {
										messagesFromClients.add(new Message(result.getRemoteAddress(), data));
										sendMessagesToClients();

									} catch (IOException e) {
										throw new RuntimeException(e);
									}
								} else {
									try {
										disconnect(result.getRemoteAddress());
										break;
									} catch (IOException e) {
										throw new RuntimeException(e);
									}
								}
							}
						}
						@Override
						public void failed(Throwable exc, Object attachment) {
							serverChannel.accept(null, this);
							throw new UnsupportedOperationException("Cannot accept connections!");
						}

						private void sendMessagesToClients() {
							Message msg;
							while ((msg = messagesFromClients.poll()) != null) {
								var client = clients.get(msg.address());
								if (client == null) {
									log.severe(String.format("client %s not found", msg.address()));
								} else {
									write(client, msg.data());
								}
							}
						}

						private void write(AsynchronousSocketChannel clientChannel, Data data) {
							try {
								ByteArrayOutputStream bos = new ByteArrayOutputStream();
								data.writeDelimitedTo(bos);
								clientChannel.write(ByteBuffer.wrap(bos.toByteArray()));
							} catch (Exception ex) {
								throw new RuntimeException("Write to the client error", ex);
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

	private Data readRequest(AsynchronousSocketChannel socketChannel) {
		try {
			int usedIdx = 0;
			int readBytesTotal = 0;
			int readBytes;

			while (true) {
				ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
				Future<Integer> f = socketChannel.read(buffer);
				try {
					readBytes = f.get(1, TimeUnit.MILLISECONDS);
				} catch (TimeoutException e) {
					break;
				}
				if (readBytes <= 0)
					break;

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

	private class ReadHandler implements CompletionHandler<Integer, Ref>{
		@Override
		public void completed(Integer result, Ref attachment) {
			attachment.setO(result);
		}

		@Override
		public void failed(Throwable exc, Ref attachment) {

		}
	}

	private record Message(SocketAddress address, Data data) {}

	private class Ref {
		private Object o;

		public Ref(Object o) {
			this.o = o;
		}

		public Object getO() {
			return o;
		}

		public void setO(Object o) {
			this.o = o;
		}
	}
}

