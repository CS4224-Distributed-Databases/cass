import com.datastax.driver.core.*;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

        // Iterate through the err log files and get the stats for max, min, avg throughputs among all clients

        // USER TO INPUT THE NUM OF CLIENTS THAT WAS PASSED IN TO TEST
        int numClients = Integer.parseInt(args[1]);
        double[] result = getThroughputsStatsFromLogFiles(numClients);
        writeTotalThroughputStatsToCsv(result[0], result[1], result[2], numClients);

        close();
    }

    public static double[] getThroughputsStatsFromLogFiles(int numClients) {
        double[] results = new double[3]; // first store min, second store max, third store total

        double minThroughputPercentage = Double.MAX_VALUE;
        double maxThroughputPercentage = 0;
        double totalThroughputPercentage = 0; // to calculate average later

        for (int i = 1; i <= numClients; i++) {
            String fileName = "log/" + i + ".err.log";
            try {
                BufferedReader br = new BufferedReader(new FileReader(fileName));
                String line;
                while( (line = br.readLine() ) != null) {
                    if (line.startsWith("Transaction throughput: ")) {
                        Pattern p = Pattern.compile("(\\d+(?:\\.\\d+))");
                        Matcher m = p.matcher(line);
                        if (m.find()) {
                            double throughput = Double.parseDouble(m.group(1));
                            minThroughputPercentage = Math.min(minThroughputPercentage, throughput);
                            maxThroughputPercentage = Math.max(maxThroughputPercentage, throughput);
                            totalThroughputPercentage += throughput;
                        }
                    }
                }
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

    public static void close(){
        // close and exit
        cluster.close();
        System.exit(0);
    }

}
