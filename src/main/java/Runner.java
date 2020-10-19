import DataLoader.CreateTables;
import DataLoader.LoadData;
import Transactions.*;
import com.datastax.driver.core.*;

import java.util.*;

public class Runner {

    private static Cluster cluster;
    private static final double convertSecondsDenom = 1000000000.0;
    private static final double convertMilliSecondsDenom = 1000000.0;

    public static void main(String[] args) throws Exception {

        // (1) Initialise Cluster
        cluster = Cluster.builder().addContactPoint("127.0.0.1")
                .build();

        cluster.init();

        Session session = cluster.connect();

        // Create Keyspace called CS4224
        String createKeyspaceQuery = "CREATE KEYSPACE IF NOT EXISTS CS4224 WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':3};";

        session.execute(createKeyspaceQuery);

        String useKeyspace = "USE CS4224";

        session.execute(useKeyspace);

        System.out.println("Keyspace CS4224 created");

        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s %s\n",
                metadata.getClusterName(), cluster.getClusterName());

        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Data Center: %s; Rack: %s; Host: %s\n",
                    host.getDatacenter(), host.getRack(), host.getAddress());
        }

        System.out.printf("Protocol Version: %s\n",
                cluster.getConfiguration()
                        .getProtocolOptions()
                        .getProtocolVersion());


//        // (2) Create Tables
//        new CreateTables(session);
//
//        // (3) Load Data and pass session
//        LoadData a = new LoadData(session);
//        a.executeLoadData();
//

        // (4) Take in inputs...parser from stdin redirection.
        String consistencyLevel = args[0];
        Integer clients = Integer.parseInt(args[1]);
        System.out.println(clients);
        HashMap<String, PreparedStatement> insertPrepared = new HashMap<>();
        Scanner sc = new Scanner(System.in);
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
        double timeElapsedInSeconds = timeElapsed/convertSecondsDenom;
        Collections.sort(latencies);
        double averageLatencyInMs = getAverageLatency(latencies)/convertMilliSecondsDenom;
        double medianLatencyInMs = getMedianLatency(latencies)/convertMilliSecondsDenom;
        double percentileLatency95InMs = getPercentileLatency(latencies, 95)/convertMilliSecondsDenom;
        double percentileLatency99InMs = getPercentileLatency(latencies, 99)/convertMilliSecondsDenom;

        printPerformance(numOfTransactions, timeElapsedInSeconds, averageLatencyInMs, medianLatencyInMs, percentileLatency95InMs, percentileLatency99InMs);

        close();
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

    private static void printPerformance(int numOfTransactions, double timeElapsedInSeconds, double averageLatencyInMs, double medianLatencyInMs, double percentileLatency95InMs, double percentileLatency99InMs) {
        System.out.println("---------------- Performance Output ----------------");
        System.out.println("Number of executed transactions: " + numOfTransactions);
        System.out.println(String.format("Total transaction execution time (sec): %.2f", timeElapsedInSeconds));
        System.out.println(String.format("Transaction throughput: %.2f", numOfTransactions/timeElapsedInSeconds));
        System.out.println(String.format("Average transaction latency (ms): %.2f", averageLatencyInMs));
        System.out.println(String.format("Median transaction latency (ms): %.2f", medianLatencyInMs));
        System.out.println(String.format("95th percentile transaction latency (ms): %.2f", percentileLatency95InMs));
        System.out.println(String.format("99th percentile transaction latency (ms): %.2f", percentileLatency99InMs));
        System.out.println("----------------------------------------------------");
    }

    public static void close(){
        // close and exit
        cluster.close();
        System.exit(0);
    }

}
