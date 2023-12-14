package pro.karagodin.server;

public class BlockingServer implements Runnable {

	@Override
	public void run() {
		System.out.println("Blocking Server run");
	}
}
