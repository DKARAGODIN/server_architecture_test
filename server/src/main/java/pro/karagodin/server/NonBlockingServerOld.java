package pro.karagodin.server;

import pro.karagodin.message.Data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

@Deprecated
public class NonBlockingServerOld implements Runnable {

	private static final Logger log = Logger.getLogger(BlockingServer.class.getName());

	private final ExecutorService processor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	private final ExecutorService sender = Executors.newSingleThreadExecutor();

	@Override
	public void run() {
		try {
			Selector selector = Selector.open();
			ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
			ServerSocket serverSocket = serverSocketChannel.socket();
			serverSocket.bind(new InetSocketAddress("localhost", Main.SERVER_PORT));
			serverSocketChannel.configureBlocking(false);
			int ops = serverSocketChannel.validOps();
			serverSocketChannel.register(selector, ops, null);
			while (true) {
				selector.select();
				Set<SelectionKey> selectedKeys = selector.selectedKeys();
				Iterator<SelectionKey> i = selectedKeys.iterator();
				while (i.hasNext()) {
					SelectionKey key = i.next();
					if (key.isAcceptable()) {
						System.out.println("Connection Accepted..");
						SocketChannel client = serverSocketChannel.accept();
						client.configureBlocking(false);
						SelectionKey clientKey=client.register(selector, SelectionKey.OP_READ);
						ByteBuffer buffer = ByteBuffer.allocate(1024);
						clientKey.attach(buffer);
					}
					else if (key.isReadable()) {
						System.out.println("Reading client's message.");
						SocketChannel channel = (SocketChannel)key.channel();
						ByteBuffer buff = (ByteBuffer) key.attachment();
						ByteArrayOutputStream out = new ByteArrayOutputStream();
						while (channel.read(buff) > 0) {
							out.write(buff.array(), 0, buff.position());
							buff.clear();
						}
						try (ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray())) {
							Data data = Data.parseDelimitedFrom(in);
							Runnable runnable = () -> {
								Data sendData = Sorter.bubleSort(data);
								Runnable send = () -> {
									try {
										ByteArrayOutputStream bos = new ByteArrayOutputStream();
										sendData.writeDelimitedTo(bos);
										channel.write(ByteBuffer.wrap(bos.toByteArray()));
										channel.close();
									} catch (IOException e) {
										e.printStackTrace();
									}
								};
								sender.submit(send);
							};
							processor.submit(runnable);
						}
					}
					i.remove();
				}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
