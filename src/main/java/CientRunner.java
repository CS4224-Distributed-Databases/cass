// NOT IN USE

import com.datastax.driver.core.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
public class CientRunner {

    private static Cluster cluster;

    public static void main(String[] args) throws Exception {

        // (1) Initialise Cluster
        cluster = Cluster.builder()
                .addContactPoint("192.168.48.169")
                .addContactPoint("192.168.48.170")
                .addContactPoint("192.168.48.171")
                .addContactPoint("192.168.48.172")
                .addContactPoint("192.168.48.173")
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(50000)) // 50 seconds
                .build();
//        cluster = Cluster.builder().addContactPoint("127.0.0.1")
//                .build();

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

        // (4) Take in inputs...parser from stdin redirection.
        String consistencyLevel = args[0];
        Integer numClients = Integer.parseInt(args[1]);
        ExecutorService executor = Executors.newFixedThreadPool(Math.max(1, numClients));
        List<Future<List<Double>>> futureResultsList = new ArrayList<>();

        if (numClients <= 0) {
            // read from command line
            executor.submit(new ClientThread(0, consistencyLevel, session));
        } else {
            // read from xact files
            for (int i = 1; i <= numClients; i++) {
                Future<List<Double>> future = executor.submit(new ClientThread(i, consistencyLevel, session));
                futureResultsList.add(future);
            }
        }

        double minThroughputPercentage = Double.MAX_VALUE;
        double totalThroughputPercentage = 0; // to calculate average later
        double maxThroughputPercentage = 0;

        /*
            PROCESS FUTURES AND WRITE TO CSV
         */
        try (PrintWriter writer = new PrintWriter(new File("src/output/client_stats.csv"))) {
            StringBuilder sb = new StringBuilder();
            // Key in the experiment number manually in a separate csv
            // 1-4 for Cassandra, 5-8 for Cockroach
//          sb.append("experiment_number");
//          sb.append(',');
            sb.append("client_number");
            sb.append(',');
            sb.append("Number of executed transactions");
            sb.append(',');
            sb.append("Total transaction execution time (sec)");
            sb.append(',');
            sb.append("Transaction throughput");
            sb.append(',');
            sb.append("Average transaction latency (ms)");
            sb.append(',');
            sb.append("Median transaction latency (ms)");
            sb.append(',');
            sb.append("95th percentile transaction latency (ms)");
            sb.append(',');
            sb.append("99th percentile transaction latency (ms)");
            sb.append('\n');

            for (Future<List<Double>> future : futureResultsList) {
                List<Double> results = future.get();
                int clientNumber = results.get(0).intValue();
                int numTxnExecuted = results.get(1).intValue();
                sb.append(clientNumber);
                sb.append(',');
                sb.append(numTxnExecuted);

                // throughput logging
                int throughput = results.get(3).intValue();
                minThroughputPercentage = Math.min(minThroughputPercentage, throughput);
                totalThroughputPercentage += throughput;
                maxThroughputPercentage = Math.max(maxThroughputPercentage, throughput);

                for (int i = 2; i < 8; i++) {
                    sb.append(',');
                    sb.append(results.get(i));
                }
                sb.append('\n');

            }
            writer.write(sb.toString());

            System.out.println("done writing to output/client_stats.csv");

        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            // no point to output performance result if error occurs
            return;
        }

        /*
            THROUGHPUT STATS
         */
        try (PrintWriter writer = new PrintWriter(new File("src/output/throughput_stats.csv"))) {
            StringBuilder sb = new StringBuilder();
            // Key in the experiment number manually in a separate csv
            // 1-4 for Cassandra, 5-8 for Cockroach
//          sb.append("experiment_number");
//          sb.append(',');
            sb.append("min throughput");
            sb.append(',');
            sb.append("avg throughput");
            sb.append(',');
            sb.append("max throughput");
            sb.append('\n');

            sb.append(minThroughputPercentage);
            sb.append(',');
            sb.append(totalThroughputPercentage / numClients);
            sb.append(',');
            sb.append(maxThroughputPercentage);

            writer.write(sb.toString());

            System.out.println("done writing to output/throughput_stats.csv");
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("\nAll clients have completed their transactions.");
        close();
    }

    public static void close(){
        // close and exit
        cluster.close();
        System.exit(0);
    }

}
