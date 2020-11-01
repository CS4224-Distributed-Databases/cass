import com.datastax.driver.core.*;
import util.CqlQueries;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class EndStateRunner {

    private static Cluster cluster;

    public static void main(String[] args) {

        // (1) Initialise Cluster
        cluster = Cluster.builder()
                .addContactPoint("192.168.48.169")
                .addContactPoint("192.168.48.170")
                .addContactPoint("192.168.48.171")
                .addContactPoint("192.168.48.172")
                .addContactPoint("192.168.48.173")
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(0)) // unlimited
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

        // Output database state
        String warehouse = CqlQueries.END_STATE_WAREHOUSE;
        String district = CqlQueries.END_STATE_DISTRICT;
        String customer = CqlQueries.END_STATE_CUSTOMER;
        String order = CqlQueries.END_STATE_ORDER;
        String orderline = CqlQueries.END_STATE_ORDERLINE;
        String stock = CqlQueries.END_STATE_STOCK;
        BigDecimal sum_wtd = session.execute(warehouse).one().getDecimal(0);

        Row districtRow = session.execute(district).one();
        BigDecimal sum_d_ytd = districtRow.getDecimal(0);
        Integer sum_next_o_id = districtRow.getInt(1);

        Row customerRow = session.execute(customer).one();
        BigDecimal sum_c_balance = customerRow.getDecimal(0);
        Float sum_c_ytd_payment = customerRow.getFloat(1);
        Integer sum_c_payment_cnt = customerRow.getInt(2);
        Integer sum_c_delivery_cnt = customerRow.getInt(3);

        Row orderRow = session.execute(order).one();
        Integer sum_o_id = orderRow.getInt(0);
        BigDecimal sum_o_ol_cnt = orderRow.getDecimal(1);

        Row orderlineRow = session.execute(orderline).one();
        BigDecimal sum_ol_amount = orderlineRow.getDecimal(0);
        BigDecimal sum_ol_quantity = orderlineRow.getDecimal(1);

        Row stockRow = session.execute(stock).one();
        BigDecimal sum_s_quantity = stockRow.getDecimal(0);
        BigDecimal sum_s_ytd = stockRow.getDecimal(1);
        Integer sum_s_order_cnt = stockRow.getInt(2);
        Integer sum_s_remote_cnt = stockRow.getInt(3);

        try (PrintWriter writer = new PrintWriter(new File("src/output/end_state.csv"))) {
            StringBuilder sb = new StringBuilder();
            // Key in the experiment number manually in a separate csv
            // 1-4 for Cassandra, 5-8 for Cockroach
//          sb.append("experiment_number");
//          sb.append(',');
            sb.append("sum_wtd");
            sb.append(',');
            sb.append("sum_d_ytd");
            sb.append(',');
            sb.append("sum_next_o_id");
            sb.append(',');
            sb.append("sum_c_balance");
            sb.append(',');
            sb.append("sum_c_ytd_payment");
            sb.append(',');
            sb.append("sum_c_payment_cnt");
            sb.append(',');
            sb.append("sum_c_delivery_cnt");
            sb.append(',');
            sb.append("sum_o_id");
            sb.append(',');
            sb.append("sum_o_ol_cnt");
            sb.append(',');
            sb.append("sum_ol_amount");
            sb.append(',');
            sb.append("sum_ol_quantity");
            sb.append(',');
            sb.append("sum_s_quantity");
            sb.append(',');
            sb.append("sum_s_ytd");
            sb.append(',');
            sb.append("sum_s_order_cnt");
            sb.append(',');
            sb.append("sum_s_remote_cnt");
            sb.append('\n');

            sb.append(sum_wtd);
            sb.append(',');
            sb.append(sum_d_ytd);
            sb.append(',');
            sb.append(sum_next_o_id);
            sb.append(',');
            sb.append(sum_c_balance);
            sb.append(',');
            sb.append(sum_c_ytd_payment);
            sb.append(',');
            sb.append(sum_c_payment_cnt);
            sb.append(',');
            sb.append(sum_c_delivery_cnt);
            sb.append(',');
            sb.append(sum_o_id);
            sb.append(',');
            sb.append(sum_o_ol_cnt);
            sb.append(',');
            sb.append(sum_ol_amount);
            sb.append(',');
            sb.append(sum_ol_quantity);
            sb.append(',');
            sb.append(sum_s_quantity);
            sb.append(',');
            sb.append(sum_s_ytd);
            sb.append(',');
            sb.append(sum_s_order_cnt);
            sb.append(',');
            sb.append(sum_s_remote_cnt);
            sb.append('\n');

            writer.write(sb.toString());

            System.out.println("done writing to output/end_state.csv");
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }

        close();
    }

    public static void close(){
        // close and exit
        cluster.close();
        System.exit(0);
    }

}
