package pro.karagodin.server;

import java.nio.channels.SocketChannel;

public class CommunicationException extends RuntimeException {

	private final transient SocketChannel socketChannel;

	public CommunicationException(String message, Throwable cause, SocketChannel socketChannel) {
		super(message, cause);
		this.socketChannel = socketChannel;
	}

	public SocketChannel getSocketChannel() {
		return socketChannel;
	}
}
