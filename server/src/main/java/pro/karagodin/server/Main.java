package pro.karagodin.server;


import java.util.logging.Logger;

public class Main {

	public static final int SERVER_PORT = 8000;
	public static final int SERVER_RESET_PORT = 8001;

	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");
	}
	private static final Logger log = Logger.getLogger(Main.class.getName());

	public static void main(String[] args) {
		if (args.length != 1)
			throw new IllegalArgumentException("Illegal arguments");

		ServerType SERVER_TYPE = ServerType.valueOf(args[0]);
		switch (SERVER_TYPE) {
			case BLOCKING -> {
				Thread thread = new Thread(new BlockingServer());
				thread.start();
			}
			case NON_BLOCKING -> {
				Thread thread = new Thread(new NonBlockingServer());
				thread.start();
			}
			case ASYNCH -> {
				Thread thread = new Thread(new AsynchServer());
				thread.start();
			}
		}
	}

	enum ServerType {
		BLOCKING,
		NON_BLOCKING,
		ASYNCH,
	}
}
