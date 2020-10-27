import com.datastax.driver.core.*;
import util.StatsItem;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// This class iterates through the err log files
// Get the stats for max, min, avg throughputs among all clients for this experiment -> ultimately for throughputs.csv
// Get all clients performance stats for this experiment -> ultimately for clients.csv
public class TotalStatsRunner {

    private static Cluster cluster;

    public static void main(String[] args) {

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

        // USER TO INPUT THE NUM OF CLIENTS THAT WAS PASSED IN TO TEST
        int numClients = Integer.parseInt(args[1]);
        List<StatsItem> clientsStatsList = new ArrayList<>();
        double[] result = getStatsFromLogFiles(numClients, clientsStatsList);
        writeTotalThroughputStatsToCsv(result[0], result[1], result[2], numClients);
        writeClientsStatsToCsv(clientsStatsList);

        close();
    }

    public static double[] getStatsFromLogFiles(int numClients, List<StatsItem> clientsStatsList) {
        double[] results = new double[3]; // first store min, second store max, third store total

        double minThroughputPercentage = Double.MAX_VALUE;
        double maxThroughputPercentage = 0;
        double totalThroughputPercentage = 0; // to calculate average later

        for (int i = 1; i <= numClients; i++) {
            String fileName = "log/" + i + ".err.log";
            StatsItem statsForThisClient = new StatsItem();
            statsForThisClient.clientNum = (double) i;
            try {
                BufferedReader br = new BufferedReader(new FileReader(fileName));
                String line;
                while( (line = br.readLine() ) != null) {
                    if (line.startsWith("Number of executed transactions: ")) {
                        Pattern p = Pattern.compile("(\\d+(?:\\.\\d+))");
                        Matcher m = p.matcher(line);
                        if (m.find()) {
                            statsForThisClient.numOfTransactions = Double.parseDouble(m.group(1));
                        }
                    } else if (line.startsWith("Total transaction execution time (sec): ")) {
                        Pattern p = Pattern.compile("(\\d+(?:\\.\\d+))");
                        Matcher m = p.matcher(line);
                        if (m.find()) {
                            statsForThisClient.executionTime = Double.parseDouble(m.group(1));
                        }
                    } else if (line.startsWith("Transaction throughput: ")) {
                        Pattern p = Pattern.compile("(\\d+(?:\\.\\d+))");
                        Matcher m = p.matcher(line);
                        if (m.find()) {
                            double throughput = Double.parseDouble(m.group(1));
                            statsForThisClient.throughput = throughput;
                            minThroughputPercentage = Math.min(minThroughputPercentage, throughput);
                            maxThroughputPercentage = Math.max(maxThroughputPercentage, throughput);
                            totalThroughputPercentage += throughput;
                        }
                    } else if (line.startsWith("Average transaction latency (ms): ")) {
                        Pattern p = Pattern.compile("(\\d+(?:\\.\\d+))");
                        Matcher m = p.matcher(line);
                        if (m.find()) {
                            statsForThisClient.avgLatency = Double.parseDouble(m.group(1));
                        }
                    } else if (line.startsWith("Median transaction latency (ms): ")) {
                        Pattern p = Pattern.compile("(\\d+(?:\\.\\d+))");
                        Matcher m = p.matcher(line);
                        if (m.find()) {
                            statsForThisClient.medianLatency = Double.parseDouble(m.group(1));
                        }
                    } else if (line.startsWith("95th percentile transaction latency (ms): ")) {
                        Pattern p = Pattern.compile("(\\d+(?:\\.\\d+))");
                        Matcher m = p.matcher(line);
                        if (m.find()) {
                            statsForThisClient.latency95 = Double.parseDouble(m.group(1));
                        }
                    } else if (line.startsWith("99th percentile transaction latency (ms): ")) {
                        Pattern p = Pattern.compile("(\\d+(?:\\.\\d+))");
                        Matcher m = p.matcher(line);
                        if (m.find()) {
                            statsForThisClient.latency99 = Double.parseDouble(m.group(1));
                        }
                    }
                }
                clientsStatsList.add(statsForThisClient);

            } catch (IOException e) {
                e.getMessage();
            }
        }

        results[0] = minThroughputPercentage;
        results[1] = maxThroughputPercentage;
        results[2] = totalThroughputPercentage;

        return results;
    }

    public static void writeTotalThroughputStatsToCsv(double minThroughputPercentage,double maxThroughputPercentage,  double totalThroughputPercentage, int numClients) {
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
    }


    public static void writeClientsStatsToCsv(List<StatsItem> clientStatsList) {
        /*
            CLIENT STATS
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

            for (StatsItem statsResult : clientStatsList) {
                sb.append(statsResult.clientNum);
                sb.append(',');
                sb.append(statsResult.numOfTransactions);
                sb.append(',');
                sb.append(statsResult.executionTime);
                sb.append(',');
                sb.append(statsResult.throughput);
                sb.append(',');
                sb.append(statsResult.avgLatency);
                sb.append(',');
                sb.append(statsResult.medianLatency);
                sb.append(',');
                sb.append(statsResult.latency95);
                sb.append(',');
                sb.append(statsResult.latency99);
                sb.append(',');
                sb.append('\n');
            }

            writer.write(sb.toString());

            System.out.println("done writing to output/client_stats.csv");
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void close(){
        // close and exit
        cluster.close();
        System.exit(0);
    }

}
