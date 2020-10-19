import Transactions.*;
import com.datastax.driver.core.*;

import java.util.*;
import java.io.*;
import java.util.concurrent.Callable;

public class ClientThread implements Callable<List<Double>> {

    private int job;
    private String consistencyLevel;
    private Session session;

    ClientThread(int job, String consistencyLevel, Session session) {
        this.job = job;
        this.consistencyLevel = consistencyLevel;
        this.session = session;

    }
    private static final double convertSecondsDenom = 1000000000.0;
    private static final double convertMilliSecondsDenom = 1000000.0;

    @Override
    public List<Double> call() throws Exception {
        List<Double> result = new ArrayList<>();
        try {
            Scanner sc;
            if (this.job == 0) {
                // read file from command line
                sc = new Scanner(System.in);
                System.out.println("hi");
            } else {
                // read file from xact folder
                sc = new Scanner(new BufferedReader(new FileReader("src/main/java/DataSource/xact-files/" + this.job + ".txt")));

            }

            HashMap<String, PreparedStatement> insertPrepared = new HashMap<>();
            int numOfTransactions = 0;
            long startTime;
            long endTime;
            long transactionStart;
            long transactionEnd;
            List<Long> latencies = new ArrayList<>();

            startTime = System.nanoTime();
            while (sc.hasNext()) {
                String inputLine = sc.nextLine();
                BaseTransaction transaction = null;

                if (inputLine.startsWith("N")) {
                    transaction = new NewOrderTransaction(session, insertPrepared, consistencyLevel);
                } else if (inputLine.startsWith("P")) {
                    transaction = new PaymentTransaction(session, insertPrepared, consistencyLevel);
                } else if (inputLine.startsWith("D")) {
                    transaction = new DeliveryTransaction(session, insertPrepared, consistencyLevel);
                } else if (inputLine.startsWith("O")) {
                    transaction = new OrderStatusTransaction(session, insertPrepared, consistencyLevel);
                } else if (inputLine.startsWith("S")) {
                    transaction = new StockLevelTransaction(session, insertPrepared, consistencyLevel);
                } else if (inputLine.startsWith("I")) {
                    transaction = new PopularItemTransaction(session, insertPrepared, consistencyLevel);
                } else if (inputLine.startsWith("T")) {
                    transaction = new TopBalanceTransaction(session, insertPrepared, consistencyLevel);
                } else if (inputLine.startsWith("R")) {
                    transaction = new RelatedCustomersTransaction(session, insertPrepared, consistencyLevel);
                }

                if (transaction != null) {
                    numOfTransactions++;
                    transaction.parseInput(sc, inputLine);
                    //CHECK IF WE WANT TO INCLUDE parseInput time as well?
                    transactionStart = System.nanoTime();
                    transaction.execute();
                    transactionEnd = System.nanoTime();
                    latencies.add(transactionEnd - transactionStart);
                }
            }
            endTime = System.nanoTime();
            long timeElapsed = endTime - startTime;
            double timeElapsedInSeconds = timeElapsed / convertSecondsDenom;
            Collections.sort(latencies);
            double transactionThroughput = numOfTransactions / timeElapsedInSeconds;
            double averageLatencyInMs = getAverageLatency(latencies) / convertMilliSecondsDenom;
            double medianLatencyInMs = getMedianLatency(latencies) / convertMilliSecondsDenom;
            double percentileLatency95InMs = getPercentileLatency(latencies, 95) / convertMilliSecondsDenom;
            double percentileLatency99InMs = getPercentileLatency(latencies, 99) / convertMilliSecondsDenom;

            printPerformance(numOfTransactions, timeElapsedInSeconds, transactionThroughput, averageLatencyInMs, medianLatencyInMs, percentileLatency95InMs, percentileLatency99InMs);
            result.add((double) this.job);
            result.add((double) numOfTransactions);
            result.add(timeElapsedInSeconds);
            result.add(transactionThroughput);
            result.add(averageLatencyInMs);
            result.add(medianLatencyInMs);
            result.add(percentileLatency95InMs);
            result.add(percentileLatency99InMs);
        } catch (FileNotFoundException e) {
            System.out.println("Read input file error for client thread with job " + job);
            System.out.println(e.getMessage());
        }

        return result;
    }

    private static double getAverageLatency(List<Long> latencies) {
        double sum = 0.0;
        for(Long latency: latencies) {
            sum += latency;
        }
        return sum/latencies.size();
    }

    private static double getMedianLatency(List<Long> latencies) {
        int length = latencies.size();
        double medianValue;
        int index = length/2;
        if (length%2 == 0) {
            medianValue = latencies.get(index) + (latencies.get(index+1) - latencies.get(index))/2.0; //avoid overflow
        } else {
            medianValue = latencies.get(index);
        }
        return medianValue;
    }

    private static long getPercentileLatency(List<Long> latencies, int percentile) {
        int length = latencies.size();
        int index = length*(percentile/100);
        return latencies.get(index);
    }

    private static void printPerformance(int numOfTransactions, double timeElapsedInSeconds, double transactionThroughput, double averageLatencyInMs, double medianLatencyInMs, double percentileLatency95InMs, double percentileLatency99InMs) {
        System.out.println("---------------- Performance Output ----------------");
        System.out.println("Number of executed transactions: " + numOfTransactions);
        System.out.println(String.format("Total transaction execution time (sec): %.2f", timeElapsedInSeconds));
        System.out.println(String.format("Transaction throughput: %.2f", transactionThroughput));
        System.out.println(String.format("Average transaction latency (ms): %.2f", averageLatencyInMs));
        System.out.println(String.format("Median transaction latency (ms): %.2f", medianLatencyInMs));
        System.out.println(String.format("95th percentile transaction latency (ms): %.2f", percentileLatency95InMs));
        System.out.println(String.format("99th percentile transaction latency (ms): %.2f", percentileLatency99InMs));
        System.out.println("----------------------------------------------------");
    }

}
