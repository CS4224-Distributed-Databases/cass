package DataLoader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.xml.bind.DatatypeConverter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class LoadData {

    private static Map<Integer, String> itemid_to_itemname;
    private static Map<Integer, String> warehouseid_to_warehousename;
    private static Map<Integer, String> districtid_to_districtname;
    private static Map<Integer, String> warehouseid_to_warehousetax;
    private static Map<Integer, ArrayList<String>> customerid_to_customernames;
    private static Map<Integer, ArrayList<Integer>> itemid_to_orderNumbers;
    private static  Map<Integer, ArrayList<String>> itemid_to_customerNames;

    private static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                                    .appendValue(ChronoField.YEAR, 4)
                                    .appendLiteral('-')
                                    .appendPattern("MM")
                                    .appendLiteral('-')
                                    .appendPattern("dd")
                                    .appendLiteral(' ')
                                    .appendPattern("HH")
                                    .appendLiteral(':')
                                    .appendPattern("mm")
                                    .appendLiteral(':')
                                    .appendPattern("ss")
                                    .appendPattern(".")
                                    .optionalStart().appendPattern("SSS").optionalEnd() // Some time stamps might have 2 SS or 3
                                    .optionalStart().appendPattern("SS").optionalEnd() // Hence make it optional
                                    .toFormatter();

    private static DateTimeFormatter format = formatter.withZone(ZoneId.of("UTC")); //timestamp TODO must remove timezone

    private static final int limit = 100;
    private static int i = 0;
    private static Session session;
    private static String DIRECTORY = "src/main/java/DataSource/data-files/";

    public LoadData(Session session) {
        this.itemid_to_itemname = new HashMap<>();
        this.warehouseid_to_warehousename = new HashMap<>();
        this.districtid_to_districtname = new HashMap<>();
        this.warehouseid_to_warehousetax = new HashMap<>();
        this.customerid_to_customernames = new HashMap<>();
        this.itemid_to_orderNumbers = new HashMap<>();
        this.itemid_to_customerNames = new HashMap<>();
        this.session = session;
    }

    // Note that the order of execution should not be changed
    // NOTE: LoadItem() function has 2 parts
    // The reason for separation is because we need to loop through item rows to get mapping of itemID_to_itemName which is needed in loadOrderline().
    // At the same time we also need to loop through orderline and order tables for load item to create the list I_O_ID_LIST.
    // Hence, there is a cycle.
    // So Item is split into 2 parts of execution.
    public void executeLoadData() throws Exception{
        loadWarehouse();
        System.out.println("Finish Loading Warehouse Data");
        loadDistrict();
        System.out.println("Finish Loading District Data");
        loadCustomer();
        System.out.println("Finish Loading Customer Data");
        loadItemPartOne();
        System.out.println("Finish Loading Item Data - Part 1");
        loadOrderline();
        System.out.println("Finish Loading Order Line Data");
        loadOrder();
        System.out.println("Finish Loading Order Data");
        loadStock();
        System.out.println("Finish Loading Stock Data");
        loadItemPartTwo();
        System.out.println("Finish loading Item Data - Part 2");
    }

    public static void loadWarehouse() throws IOException {
        File file = new File(DIRECTORY + "warehouse.csv");
        BufferedReader br = new BufferedReader(new FileReader(file));

        // create parameterized INSERT statement
        PreparedStatement insertPrepared = session.prepare(
                "INSERT INTO Warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        BoundStatement insertBound;

        String line;
        while ((line = br.readLine()) != null) {
            i++;
            if (i == limit) {
                i = 0;
                break;
            }

            String[] row = line.split(",");

            warehouseid_to_warehousename.put(Integer.parseInt(row[0]), row[1]); // put it in hashmap to use later in customer
            warehouseid_to_warehousetax.put(Integer.parseInt(row[0]), row[7]); // put in hashmap to use later in district
            insertBound = insertPrepared.bind(Integer.parseInt(row[0]), row[1], row[2], row[3], row[4], row[5], row[6], DatatypeConverter.parseDecimal(row[7]), DatatypeConverter.parseDecimal(row[8]));

            session.execute(insertBound);
        }
    }

    public static void loadDistrict() throws IOException {
        File file = new File(DIRECTORY + "district.csv");
        BufferedReader br = new BufferedReader(new FileReader(file));

        // create parameterized INSERT statement
        PreparedStatement insertPrepared = session.prepare(
                "INSERT INTO District (D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID, W_TAX) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        BoundStatement insertBound;

        String line;
        while ((line = br.readLine()) != null) {
            i++;
            if (i == limit) {
                i = 0;
                break;
            }

            String[] row = line.split(",");

            districtid_to_districtname.put(Integer.parseInt(row[1]), row[2]); // put it in hashmap to use later in customer

            String warehouseTax = warehouseid_to_warehousetax.get(Integer.parseInt(row[1]));

            insertBound = insertPrepared.bind(Integer.parseInt(row[0]), Integer.parseInt(row[1]), row[2], row[3], row[4], row[5], row[6], row[7], DatatypeConverter.parseDecimal(row[8]), DatatypeConverter.parseDecimal(row[9]), Integer.parseInt(row[10]), DatatypeConverter.parseDecimal(warehouseTax));

            session.execute(insertBound);
        }
    }

    public static void loadCustomer() throws IOException {
        File file = new File(DIRECTORY + "customer.csv");
        BufferedReader br =  new BufferedReader(new FileReader(file));

        // create parameterized INSERT statement
        PreparedStatement insertPrepared = session.prepare(
                "INSERT INTO Customer (C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT," +
                        " C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA, C_D_NAME, C_W_NAME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        BoundStatement insertBound;

        String line;
        while ((line = br.readLine()) != null) {
            i++;
            if (i == limit) {
                i = 0;
                break;
            }

            String[] row = line.split(",");

            ArrayList<String> customerNames = new ArrayList<>(Arrays.asList(row[3], row[4], row[5]));
            customerid_to_customernames.put(Integer.parseInt(row[2]), customerNames); // To be used in Order_New Table

            String warehouseName = warehouseid_to_warehousename.get(Integer.parseInt(row[0]));
            String districtName = districtid_to_districtname.get(Integer.parseInt(row[1]));

            insertBound = insertPrepared.bind(Integer.parseInt(row[0]), Integer.parseInt(row[1]), Integer.parseInt(row[2]), row[3], row[4], row[5],
                    row[6], row[7], row[8], row[9], row[10], row[11], Timestamp.from(Instant.from(format.parse(row[12]))), row[13], DatatypeConverter.parseDecimal(row[14]),
                    DatatypeConverter.parseDecimal(row[15]),  DatatypeConverter.parseDecimal(row[16]),  Float.parseFloat(row[17]), Integer.parseInt(row[18]),
                    Integer.parseInt(row[19]), row[20], districtName, warehouseName);

            session.execute(insertBound);

        }
    }

    // This function does not execute CQL query. The main purpose is to fill the hashmap.
    public static void loadItemPartOne() throws IOException {
        File file = new File(DIRECTORY + "item.csv");
        BufferedReader br = new BufferedReader(new FileReader(file));

        String line;
        while ((line = br.readLine()) != null) {
            i++;
            if (i == limit) {
                i = 0;
                break;
            }

            String[] row = line.split(",");

            itemid_to_itemname.put(Integer.parseInt(row[0]), row[1]); // put it in hashmap to use later in loadOrderline

        }
    }

    public static void loadOrderline() throws IOException {
        File file = new File(DIRECTORY + "order-line.csv");
        BufferedReader br = new BufferedReader(new FileReader(file));

        // create parameterized INSERT statement
        PreparedStatement insertPrepared = session.prepare(
                "INSERT INTO Order_line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO, OL_I_NAME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        BoundStatement insertBound;

        String line;
        while ((line = br.readLine()) != null) {
            i++;
            if (i == limit) {
                i = 0;
                break;
            }

            String[] row = line.split(",");

            // We create hash here for loadItemPartTwo()
            ArrayList<Integer> correspondingOrders = itemid_to_orderNumbers.getOrDefault(Integer.parseInt(row[4]), new ArrayList<>());
            correspondingOrders.add(Integer.parseInt(row[2]));
            itemid_to_orderNumbers.put(Integer.parseInt(row[4]), correspondingOrders);

            int this_item = Integer.parseInt(row[4]);

            insertBound = insertPrepared.bind(Integer.parseInt(row[0]), Integer.parseInt(row[1]), Integer.parseInt(row[2]), Integer.parseInt(row[3]), Integer.parseInt(row[4]), Timestamp.from(Instant.from(format.parse(row[5]))), DatatypeConverter.parseDecimal(row[6]), Integer.parseInt(row[7]), DatatypeConverter.parseDecimal(row[8]), row[9], itemid_to_itemname.get(this_item));

            session.execute(insertBound);
        }
    }

    public static void loadOrder() throws IOException {
        File file = new File(DIRECTORY + "order.csv");
        BufferedReader br = new BufferedReader(new FileReader(file));

        // create parameterized INSERT statement
        PreparedStatement insertPrepared = session.prepare(
                "INSERT INTO Order_New (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY, " +
                        "O_C_FIRST, O_C_MIDDLE, O_C_LAST) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        BoundStatement insertBound;

        String line;
        while ((line = br.readLine()) != null) {
            i++;
            if (i == limit) {
                i = 0;
                break;
            }

            String[] row = line.split(",");

            ArrayList<String> cust = customerid_to_customernames.getOrDefault(Integer.parseInt(row[3]), new ArrayList<>());
            String firstName = "";
            String middleName = "";
            String lastName = "";

            // Need this check because we are currently loading data with limit
            if(cust.size()!=0){
                firstName = cust.get(0);
                middleName = cust.get(1);
                lastName = cust.get(2);
            }

            // Need to do this for loadData part 2.
            for (Map.Entry<Integer, ArrayList<Integer>> entry: itemid_to_orderNumbers.entrySet()){
                ArrayList<Integer> orders = entry.getValue();
                Integer item = entry.getKey();

                // We check the Map value entries for those with the current order number
                // Then we get their corresponding item id
                // And append the current customer name to the map itemid_to_customerNames
                if(orders.contains(Integer.parseInt(row[2]))){
                    ArrayList<String> allCustomers = itemid_to_customerNames.getOrDefault(item, new ArrayList<>());
                    String currentCus = firstName + " " + middleName + " " + lastName;
                    allCustomers.add(currentCus);
                    itemid_to_customerNames.put(item, allCustomers);
                }
            }

            insertBound = insertPrepared.bind(Integer.parseInt(row[0]), Integer.parseInt(row[1]), Integer.parseInt(row[2]), Integer.parseInt(row[3]), Integer.parseInt(row[4]),
                    DatatypeConverter.parseDecimal(row[5]), DatatypeConverter.parseDecimal(row[6]), Timestamp.from(Instant.from(format.parse(row[7]))), firstName, middleName, lastName);

            session.execute(insertBound);
        }
    }

    public static void loadStock() throws IOException {
        File file = new File(DIRECTORY + "stock.csv");
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

    public static void loadItemPartTwo() throws IOException {

        File file = new File(DIRECTORY + "item.csv");
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

            ArrayList<String> allCust = itemid_to_customerNames.get(Integer.parseInt(row[0]));

            insertBound = insertPrepared.bind(Integer.parseInt(row[0]), row[1], DatatypeConverter.parseDecimal(row[2]), Integer.parseInt(row[3]), row[4], allCust);

            session.execute(insertBound);
        }

    }
}

