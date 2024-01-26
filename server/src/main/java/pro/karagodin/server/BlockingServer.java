package pro.karagodin.server;

import pro.karagodin.message.Data;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class BlockingServer implements Runnable {

	private static final Logger log = Logger.getLogger(BlockingServer.class.getName());

	private final ExecutorService workers = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	private final List<Long> serverTimes = Collections.synchronizedList(new ArrayList<>());
	private final List<Double> avgSortTimes = Collections.synchronizedList(new ArrayList<>());
	private final List<ExecutorService> senders = Collections.synchronizedList(new ArrayList<>());

	@Override
	public void run() {
		startResetListener();

		int clientId = 0;
		try (ServerSocket serverSocket = new ServerSocket(Main.SERVER_PORT)) {
			while (true) {
				clientId++;
				final int finalClientId = clientId;
				Socket clientSocket = serverSocket.accept();
				log.info("Accepted new client with id " + finalClientId);
				Runnable responseSender = () -> {
					try {
						long clientTimeOnServerStart = System.currentTimeMillis();
						int requestNumber = 0;
						List<Long> sortTimes = new ArrayList<>();
						while (true) {
							Data receivedData = Data.parseDelimitedFrom(clientSocket.getInputStream());
							if (receivedData == null) {
								break;
							}
							requestNumber++;
							log.info(String.format("Processing %s request from client %s", requestNumber, finalClientId));
							Callable<Data> routine = () -> Sorter.bubleSort(receivedData);
							long sortTimeStart = System.currentTimeMillis();
							Future<Data> future = workers.submit(routine);
							Data sendData = future.get();
							long sortTimeEnd = System.currentTimeMillis();
							long sortTime = sortTimeEnd - sortTimeStart;
							sortTimes.add(sortTime);
							log.info(String.format("Finished processing %s request from client %s", requestNumber, finalClientId));
							sendData.writeDelimitedTo(clientSocket.getOutputStream());
							log.info(String.format("Send response for %s request from client %s", requestNumber, finalClientId));
						}
						long clientTimeOnServerEnd = System.currentTimeMillis();
						long clientTimeOnServer = clientTimeOnServerEnd - clientTimeOnServerStart;
						serverTimes.add(clientTimeOnServer);
						avgSortTimes.add(sortTimes.stream().mapToLong(val -> val).average().orElse(0.0));

						double avgServerTime = serverTimes.stream().mapToLong(val -> val).average().orElse(0.0);
						double avgSortTime = avgSortTimes.stream().mapToDouble(val -> val).average().orElse(0.0);
						log.info(String.format("Processed all %s requests for client %s. Average server time per client - %f. Average sort time - %f",
								requestNumber, finalClientId, avgServerTime, avgSortTime));
						clientSocket.close();
					} catch (Throwable e) {
						e.printStackTrace();
					}
				};
				ExecutorService responser = Executors.newSingleThreadExecutor();
				responser.submit(responseSender);
				responser.shutdown();
				senders.add(responser);
				senders.removeIf(ExecutorService::isTerminated);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void startResetListener() {
		Runnable resetStatsServer = () -> {
			while(true) {
				try (ServerSocket serverSocket = new ServerSocket(Main.SERVER_RESET_PORT);
					 Socket socket = serverSocket.accept()) {
					log.info("Received reset call");
					senders.forEach(ExecutorService::shutdown);
					senders.forEach(es -> {
						try {
							es.awaitTermination(1, TimeUnit.HOURS);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
					});
					senders.removeIf(ExecutorService::isTerminated);

					double avgServerTime = serverTimes.stream().mapToLong(val -> val).average().orElse(0.0);
					double avgSortTime = avgSortTimes.stream().mapToDouble(val -> val).average().orElse(0.0);
					String line = String.format("Average server time per client - %f. Average sort time - %f", avgServerTime, avgSortTime);
					try (BufferedWriter writer = new BufferedWriter(new FileWriter("blocking_server_result", true))) {
						writer.append(line);
						writer.newLine();
					}

					serverTimes.clear();
					avgSortTimes.clear();
					log.info("Reset finished");
					try (BufferedWriter writer = new BufferedWriter(new FileWriter("blocking_server_result", true))) {
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
}
