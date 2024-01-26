package pro.karagodin.client;


import pro.karagodin.message.Data;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Main {

	private static final int SERVER_PORT = 8000;
	private static final int SERVER_RESET_PORT = 8001;
	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT.%1$tL %4$s %2$s %5$s%6$s%n");
	}
	private static final Logger log = Logger.getLogger(Main.class.getName());

	public static void main(String[] args) {
		if (args.length == 4) {
			doWork(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
			System.exit(0);
		}
		int m = 0; //Number of clients
		int n = 0; //Number of elements to sort
		int x = 0; //Number of requests
		int d = 0; //Sleep time between requests in ms

		int start = 0;
		int end = 0;
		int step = 0;

		Scanner scanner = new Scanner(System.in);
		System.out.print("Enter X value (number of requests per client) - ");
		x = scanner.nextInt();
		System.out.print("Which parameter will scale N, M or D? - ");
		String letter = scanner.next();
		switch (letter) {
			case "N" -> {
				System.out.print("Enter start value for N (Number of elements to sort) - ");
				start = scanner.nextInt();
				System.out.print("Enter end value for N (Number of elements to sort) - ");
				end = scanner.nextInt();
				System.out.print("Enter step value for N (Number of elements to sort) - ");
				step = scanner.nextInt();
				System.out.print("Enter M value (Number of parallel clients) - ");
				m = scanner.nextInt();
				System.out.print("Enter D value (Sleep time between requests) - ");
				d = scanner.nextInt();

				for (int value = start; value < end; value += step) {
					doWork(m, value, x, d);
					signalResetStats();
				}
			}
			case "M" -> {
				System.out.print("Enter start value for M (Number of parallel clients) -");
				start = scanner.nextInt();
				System.out.print("Enter end value for M (Number of parallel clients) - ");
				end = scanner.nextInt();
				System.out.print("Enter step value for M (Number of parallel clients) - ");
				step = scanner.nextInt();
				System.out.print("Enter N value (Number of elements to sort) - ");
				n = scanner.nextInt();
				System.out.print("Enter D value (Sleep time between requests) - ");
				d = scanner.nextInt();

				for (int value = start; value < end; value += step) {
					doWork(value, n, x, d);
					signalResetStats();
				}
			}
			case "D" -> {
				System.out.print("Enter start value for D (Sleep time between requests) -");
				start = scanner.nextInt();
				System.out.print("Enter end value for D (Sleep time between requests) - ");
				end = scanner.nextInt();
				System.out.print("Enter step value for D (Sleep time between requests) - ");
				step = scanner.nextInt();
				System.out.print("Enter M value (Number of parallel clients) - ");
				m = scanner.nextInt();
				System.out.print("Enter N value (Number of elements to sort) - ");
				n = scanner.nextInt();

				for (int value = start; value < end; value += step) {
					doWork(m, n, x, value);
					signalResetStats();
				}
			}
			default ->
				throw new IllegalArgumentException("Entered illegal character");
		}
	}

	private static void doWork(int m, int n, int x, int d) {
		if (n <= 0 || m <= 0 || d <= 0 || x <= 0) {
			throw new IllegalArgumentException("One of the parameters is less or equal to zero");
		}


		ExecutorService executor = Executors.newFixedThreadPool(m);
		List<Long> clientTimes = Collections.synchronizedList(new ArrayList<>());
		for (int i = 0; i < m; i++) {
			final int clientId = i + 1;

			Runnable runnable = () -> {
				log.info(clientId + " starting client");
				long startTime = System.currentTimeMillis();
				try (Socket socket = new Socket("localhost", SERVER_PORT)) {
					Random random = new Random();
					for (int j = 0; j < x; j++) {
						Data sendData = Data.newBuilder().addAllData(random.ints().limit(n).boxed().toList()).build();
						log.info(clientId + " Sending data to server");
						sendData.writeDelimitedTo(socket.getOutputStream());
						log.info(clientId + " Receiving data from server");
						Data.parseDelimitedFrom(socket.getInputStream());
						log.info(clientId + " Received data from server");
						if (j + 1 < x)
							Thread.sleep(d);
					}
				} catch (Throwable e) {
					e.printStackTrace();
					System.exit(1);
				}
				long endTime = System.currentTimeMillis();
				long clientTime = endTime - startTime;
				clientTimes.add(clientTime);
				log.info(clientId + " client finished. Client time - " + clientTime);
			};
			executor.submit(runnable);
		}
		log.info("All clients started");

		try {
			executor.shutdown();
			boolean result = executor.awaitTermination(1, TimeUnit.HOURS);
			if (!result) {
				executor.shutdownNow();
				log.severe("Clients not finished on time");
			} else {
				double avg = clientTimes.stream().mapToLong(val -> val).average().orElse(0.0);
				String line = String.format("Client run with params: n=%s, m=%s, d=%s, x=%s. Average client time %f", n, m, d, x, avg);
				try (BufferedWriter writer = new BufferedWriter(new FileWriter("client_result", true))) {
					writer.append(line);
					writer.newLine();
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	private static void signalResetStats() {
		try (Socket s = new Socket("localhost", SERVER_RESET_PORT)) {
			s.getInputStream().read();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
