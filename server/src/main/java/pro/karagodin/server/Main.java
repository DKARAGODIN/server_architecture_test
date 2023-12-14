package pro.karagodin.server;


public class Main {

	public static ServerType SERVER_TYPE = ServerType.BLOCKING;
	public static final int SERVER_PORT = 8000;

	public static void main(String[] args) {
		if (args.length != 1)
			throw new IllegalArgumentException("Illegal arguments");

		SERVER_TYPE = ServerType.valueOf(args[0]);
		switch (SERVER_TYPE) {
			case BLOCKING -> {
				Thread thread = new Thread(new BlockingServer());
				thread.start();
			}
			case NON_BLOCKING -> System.out.println("NON_BLOCKING server is not supported");
			case ASYNCH -> System.out.println("ASYNCH server is not supported");
		}
	}

	enum ServerType {
		BLOCKING,
		NON_BLOCKING,
		ASYNCH,
	}
}
