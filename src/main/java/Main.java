import DataLoader.CreateTables;
import DataLoader.LoadData;
import Transactions.DeliveryTransaction;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

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


        // (4) Take in inputs...parser
        // TODO: Add code for parser

        close();
    }

    public static void close(){
        // close and exit
        cluster.close();
        System.exit(0);
    }

}
