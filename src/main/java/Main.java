import DataLoader.CreateTables;
import DataLoader.LoadData;
import Transactions.*;

import com.datastax.driver.core.*;

import java.util.HashMap;
import java.util.Scanner;

public class Main {

    private static Cluster cluster;
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

        // (2) Create Tables
        new CreateTables(session);

        // (3) Load Data and pass session
        LoadData a = new LoadData(session);
        a.executeLoadData();

        // (4) Take in inputs...parser from stdin redirection.
        HashMap<String, PreparedStatement> insertPrepared = new HashMap<>();
        Scanner sc = new Scanner(System.in);
        while (sc.hasNext()) {
            String inputLine = sc.nextLine();
            BaseTransaction transaction = null;

            if(inputLine.startsWith("N")) {
                transaction = new NewOrderTransaction(session, insertPrepared);
            } else if (inputLine.startsWith("P")) {
                transaction = new PaymentTransaction(session, insertPrepared);
            } else if (inputLine.startsWith("D")) {
                transaction = new DeliveryTransaction(session, insertPrepared);
            } else if (inputLine.startsWith("O")) {
                //transaction = new OrderStatusTransaction(session, insertPrepared);
            } else if (inputLine.startsWith("S")) {
                //transaction = new StockLevelTransaction(session, insertPrepared);
            } else if (inputLine.startsWith("I")) {
                //transaction = new PopularItemTransaction(session, insertPrepared);
            } else if (inputLine.startsWith("T")) {
                //transaction = new TopBalanceTransaction(session, insertPrepared);
            } else if (inputLine.startsWith("R")) {
                transaction = new RelatedCustomersTransaction(session, insertPrepared);
            }

            if (transaction != null) {
                transaction.parseInput(sc, inputLine);
                transaction.execute();
            }
        }

        close();
    }

    public static void close(){
        // close and exit
        cluster.close();
        System.exit(0);
    }

}
