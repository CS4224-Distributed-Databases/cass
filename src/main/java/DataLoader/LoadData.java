package DataLoader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import javax.xml.bind.DatatypeConverter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;


public class LoadData {

    private static Map<Integer, String> itemid_to_itemname = new HashMap<>();
    private static final int limit = 100;
    private static int i = 0;
    private Session session;

    public LoadData(Session session) {
       this.session = session;
    }

    public void createTables() throws Exception{
        loadItem(session); // TODO i_o_id_list not completed
        loadOrderline(session);
        loadStock(session);
    }

    public static void loadItem(Session session) throws IOException {
        File file = new File("data/data-files/item.csv");
        BufferedReader br = new BufferedReader(new FileReader(file));

        // create parameterized INSERT statement
        PreparedStatement insertPrepared = session.prepare(
                "INSERT INTO Item (I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA, I_O_ID_LIST) VALUES (?, ?, ?, ?, ?, ?)");
        BoundStatement insertBound;

        String line;
        while ((line = br.readLine()) != null) {
            i++;
            if (i == limit) {
                i = 0;
                break;
            }

            String[] row = line.split(",");

            itemid_to_itemname.put(Integer.parseInt(row[0]), row[1]); // put it in hashmap to use later in loadOrderline

            insertBound = insertPrepared.bind(Integer.parseInt(row[0]), row[1], DatatypeConverter.parseDecimal(row[2]), Integer.parseInt(row[3]), row[4], new HashSet<String>(Collections.singleton("to-do")));

            session.execute(insertBound);
        }
    }


    public static void loadOrderline(Session session) throws IOException {
        File file = new File("data/data-files/order-line.csv");
        BufferedReader br = new BufferedReader(new FileReader(file));

        // create parameterized INSERT statement
        PreparedStatement insertPrepared = session.prepare(
                "INSERT INTO Order_line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER,  OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO, I_NAME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        BoundStatement insertBound;

        String line;
        while ((line = br.readLine()) != null) {
            i++;
            if (i == limit) {
                i = 0;
                break;
            }

            String[] row = line.split(",");

            DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC")); //timestamp TODO must remove timezone

            int this_item = Integer.parseInt(row[4]);
            insertBound = insertPrepared.bind(Integer.parseInt(row[0]), Integer.parseInt(row[1]), Integer.parseInt(row[2]), Integer.parseInt(row[3]), Integer.parseInt(row[4]), Timestamp.from(Instant.from(format.parse(row[5]))), DatatypeConverter.parseDecimal(row[6]), Integer.parseInt(row[7]), DatatypeConverter.parseDecimal(row[8]), row[9], itemid_to_itemname.get(this_item));

            session.execute(insertBound);
        }
    }

    public static void loadStock(Session session) throws IOException {
        File file = new File("data/data-files/stock.csv");
        BufferedReader br = new BufferedReader(new FileReader(file));

        // create parameterized INSERT statement
        PreparedStatement insertPrepared = session.prepare(
                "INSERT INTO Stock (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        BoundStatement insertBound;

        String line;
        while ((line = br.readLine()) != null) {
            i++;
            if (i == limit) {
                i = 0;
                break;
            }

            String[] row = line.split(",");

            insertBound = insertPrepared.bind(Integer.parseInt(row[0]), Integer.parseInt(row[1]), DatatypeConverter.parseDecimal(row[2]), DatatypeConverter.parseDecimal(row[3]), Integer.parseInt(row[4]), Integer.parseInt(row[5]), row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16]);

            session.execute(insertBound);
        }
    }
}

