package util;

// Note: Cassandra will order the partition keys and the clustering keys (ordered by their precedence in the
// PRIMARY KEY definition), and then the columns follow in ascending order.
// Hence the ordering of the columns in createTable is not followed.
// Ensure that the indexes constants created below follow the correct one.

public class CqlQueries {
    // -------------NEW ORDER TRANSACTION ----------------------------------------------------------------------------

    public static final String N_GET_DISTRICT_INFO = "SELECT D_NEXT_O_ID, D_TAX, W_TAX FROM District WHERE D_W_ID = ? AND D_ID = ?";
    public static final String N_UPDATE_DISTRICT_NEXT_O_ID = "UPDATE District SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ?";
    public static final String N_GET_CUSTOMER_INFO = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_CREDIT, C_DISCOUNT FROM Customer WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";
    public static final String N_GET_STOCK_INFO = "SELECT * FROM Stock WHERE S_W_ID = ? AND S_I_ID = ?";
    public static final String N_UPDATE_STOCK_QUANTITY = "UPDATE Stock SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_W_ID = ? AND S_I_ID = ?";
    public static final String N_GET_ITEM_INFO = "SELECT I_PRICE, I_NAME FROM Item WHERE I_ID = ?";
    public static final String N_CREATE_ORDER_LINE = "INSERT INTO Order_Line (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO, OL_I_NAME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    public static final String N_CREATE_ORDER = "INSERT INTO Order_New (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY, O_OL_CNT, O_ALL_LOCAL, O_C_FIRST, O_C_MIDDLE, O_C_LAST) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    public static final String N_CREATE_ORDER_SMALL = "INSERT INTO Order_Small (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY, O_C_FIRST, O_C_MIDDLE, O_C_LAST) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    public static final String N_UPDATE_ITEM_CUSTOMER_LIST = "UPDATE Item SET I_O_ID_LIST = I_O_ID_LIST + ? WHERE I_ID = ?";

    // Indexes for New Order Transaction
    public static final int N_D_NEXT_O_ID_INDEX = 0;
    public static final int N_D_TAX_INDEX = 1;
    public static final int N_W_TAX_INDEX = 2;

    public static final int N_C_FIRST_INDEX = 0;
    public static final int N_C_MIDDLE_INDEX = 1;
    public static final int N_C_LAST_INDEX = 2;
    public static final int N_C_CREDIT_INDEX = 3;
    public static final int N_C_DISCOUNT_INDEX = 4;

    public static final int N_I_PRICE_INDEX = 0;
    public static final int N_I_NAME_INDEX = 1;


    // -------------PAYMENT TRANSACTION ----------------------------------------------------------------------------

    public static final String P_GET_WAREHOUSE_INFO = "SELECT W_YTD, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM Warehouse WHERE W_ID = ?";
    public static final String P_UPDATE_WAREHOUSE_PAYMENT = "UPDATE Warehouse SET W_YTD = ? WHERE W_ID = ?";
    public static final String P_GET_DISTRICT_INFO = "SELECT D_YTD, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM District WHERE D_W_ID = ? AND D_ID = ?";
    public static final String P_UPDATE_DISTRICT_PAYMENT = "UPDATE District SET D_YTD = ? WHERE D_W_ID = ? AND D_ID = ?";
    public static final String P_GET_CUSTOMER_INFO = "SELECT C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT FROM Customer WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";
    public static final String P_UPDATE_CUSTOMER_PAYMENT = "UPDATE Customer SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";

    // Indexes for Payment Transaction
    public static final int PAYMENT_W_YTD_INDEX = 0;
    public static final int PAYMENT_W_STREET_1_INDEX = 1;
    public static final int PAYMENT_W_STREET_2_INDEX = 2;
    public static final int PAYMENT_W_CITY_INDEX = 3;
    public static final int PAYMENT_W_STATE_INDEX = 4;
    public static final int PAYMENT_W_ZIP_INDEX = 5;

    public static final int PAYMENT_D_YTD_INDEX = 0;
    public static final int PAYMENT_D_STREET_1_INDEX = 1;
    public static final int PAYMENT_D_STREET_2_INDEX = 2;
    public static final int PAYMENT_D_CITY_INDEX = 3;
    public static final int PAYMENT_D_STATE_INDEX = 4;
    public static final int PAYMENT_D_ZIP_INDEX = 5;

    public static final int PAYMENT_C_BALANCE_INDEX = 0;
    public static final int PAYMENT_C_YTD_PAYMENT_INDEX = 1;
    public static final int PAYMENT_C_PAYMENT_CNT_INDEX = 2;
    public static final int PAYMENT_C_FIRST_INDEX = 3;
    public static final int PAYMENT_C_MIDDLE_INDEX = 4;
    public static final int PAYMENT_C_LAST_INDEX = 5;
    public static final int PAYMENT_C_STREET_1_INDEX = 6;
    public static final int PAYMENT_C_STREET_2_INDEX = 7;
    public static final int PAYMENT_C_CITY_INDEX = 8;
    public static final int PAYMENT_C_STATE_INDEX = 9;
    public static final int PAYMENT_C_ZIP_INDEX = 10;
    public static final int PAYMENT_C_PHONE_INDEX = 11;
    public static final int PAYMENT_C_SINCE_INDEX = 12;
    public static final int PAYMENT_C_CREDIT_INDEX = 13;
    public static final int PAYMENT_C_CREDIT_LIM_INDEX = 14;
    public static final int PAYMENT_C_DISCOUNT_INDEX = 15;

    // -------------Order status TRANSACTION ----------------------------------------------------------------------------
    public static final String O_GET_CUSTOMER_INFO = "SELECT C_BALANCE, C_FIRST, C_MIDDLE, C_LAST FROM Customer WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";
    public static final String O_GET_CUSTOMER_LAST_ORDER = "select o_id, o_carrier_id, o_entry from order_small where o_w_id= ? and o_d_id= ? and o_c_id= ? order by o_id desc limit 1 allow filtering";
    public static final String O_GET_LAST_ORDER_DETAILS = "select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line where ol_w_id= ? and ol_d_id= ? and ol_o_id= ?";

    // Indexes for Order status Transaction
    public static final int ORDER_C_BALANCE_INDEX = 0;
    public static final int ORDER_C_FIRST_INDEX = 1;
    public static final int ORDER_C_MIDDLE_INDEX = 2;
    public static final int ORDER_C_LAST_INDEX = 3;
    public static final int ORDER_O_ORDER_ID_INDEX = 0;
    public static final int ORDER_O_CARRIER_ID_INDEX = 1;
    public static final int ORDER_O_ENTRY_INDEX = 2;
    public static final int ORDER_OL_ITEM_ID_INDEX = 0;
    public static final int ORDER_OL_SUPPLY_WAREHOUSE_ID_INDEX = 1;
    public static final int ORDER_OL_QUANTITY_INDEX = 2;
    public static final int ORDER_OL_AMOUNT_INDEX = 3;
    public static final int ORDER_OL_DELIVERY_DATE_INDEX = 4;

    // -------------Stock level TRANSACTION ----------------------------------------------------------------------------
    public static final String S_GET_DISTRICT = "SELECT d_next_o_id FROM district where d_w_id = ? AND d_id = ?";
    public static final String S_GET_LAST_L_ORDERS = "select ol_i_id from order_line where ol_w_id= ? and ol_d_id= ? and ol_o_id>= ? and ol_o_id<? ALLOW FILTERING";
    public static final String S_GET_STOCK_ITEMS = "select s_quantity from stock where s_w_id = ? and s_i_id = ?";

    // Indexes for Stock Level Transaction
    public static final int STOCK_D_NEXT_OID_INDEX = 0;
    public static final int STOCK_OL_OID_INDEX = 0;
    public static final int STOCK_QUANTITY_INDEX = 0;

    // -------------DELIVERY TRANSACTION ----------------------------------------------------------------------------
    public static String YET_DELIVERED_ORDER
            = "SELECT * FROM Order_New WHERE O_W_ID = ? AND O_D_ID = ? LIMIT 1;";
    public static final String UPDATE_YET_DELIVERED_ORDER
            = "UPDATE Order_New SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_C_ID = ? AND O_W_ID = ? AND O_D_ID = ? IF EXISTS;";
    public static final String GET_ORDER_LINES = "SELECT * FROM Order_Line WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?";
    public static final String UPDATE_ORDER_LINES_DELIVERY_DATE
            = "UPDATE Order_Line SET OL_DELIVERY_D = ? WHERE OL_NUMBER = ? AND OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?";
    public static final String GET_CUSTOMER_BALANCE_AND_DELIVERY_COUNT
            = "SELECT C_BALANCE, C_DELIVERY_CNT FROM Customer WHERE C_ID = ? AND C_W_ID = ? AND C_D_ID = ?";
    public static final String UPDATE_CUSTOMER_BALANCE_AND_DELIVERY_COUNT
            = "UPDATE Customer SET c_balance = ?, c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";

    // Indexes for Delivery Transaction
    public static final int DELIVERY_O_ID_INDEX = 2;
    public static final int DELIVERY_O_C_ID = 3;

    public static final int DELIVERY_OL_NUMBER = 3;
    public static final int DELIVERY_OL_AMOUNT = 4;

    // -------------Related Customers TRANSACTION ----------------------------------------------------------------------------
    public static String GET_CUSTOMER_ORDERS = "SELECT O_ID from Order_New WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ?";
    public static String GET_ITEM_NUMBER_FROM_ORDER_LINE = "SELECT OL_I_ID from Order_Line WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?";
    public static String GET_CUS_LIST_FROM_ITEM = "SELECT I_O_ID_LIST from Item WHERE I_ID = ?";

  
    // -------------TOP BALANCE TRANSACTION ----------------------------------------------------------------------------
    public static String GET_CUSTOMERS_ORDERED_BY_BALANCE = "SELECT C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST, C_W_NAME, C_D_NAME from Customer_Balance WHERE C_W_ID = ? AND C_D_ID = ? ORDER BY C_BALANCE DESC LIMIT 10;";

    // Indexes for Top Balance Transaction
    public static final int T_C_ID_INDEX = 0;
    public static final int T_C_BALANCE_INDEX = 1;
    public static final int T_C_FIRST_INDEX = 2;
    public static final int T_C_MIDDLE_INDEX = 3;
    public static final int T_C_LAST_INDEX = 4;
    public static final int T_C_W_NAME_INDEX = 5;
    public static final int T_C_D_NAME_INDEX = 6;
  
    // -------------POPULAR ITEM TRANSACTION ----------------------------------------------------------------------------
    //Uses Order Small instead of Order
    public static String I_GET_DISTRICT_NEXT_ORDER_NUM = "SELECT D_NEXT_O_ID from District WHERE D_W_ID = ? AND D_ID = ?";
    public static String I_GET_ORDER_INFO = "SELECT O_ENTRY, O_C_FIRST, O_C_MIDDLE, O_C_LAST FROM Order_Small WHERE O_W_ID = ? AND O_D_ID = ? AND O_ID = ?";
    public static String I_GET_ORDER_LINES = "SELECT OL_I_ID, OL_I_NAME, OL_QUANTITY FROM Order_Line WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?";

    // Indexes for Popular Item Transaction
    public static final int I_D_NEXT_O_ID_INDEX = 0;
    public static final int I_O_ENTRY_INDEX = 0;
    public static final int I_O_C_FIRST_INDEX = 1;
    public static final int I_O_C_MIDDLE_INDEX = 2;
    public static final int I_O_C_LAST_INDEX = 3;
    public static final int I_OL_I_ID_INDEX = 0;
    public static final int I_OL_I_NAME_INDEX = 1;
    public static final int I_OL_QUANTITY_INDEX = 2;


    // -------------END STATE QUERIES ----------------------------------------------------------------------------
    public static final String END_STATE_WAREHOUSE = "select sum(w_ytd) from warehouse";
    public static final String END_STATE_DISTRICT = "select sum(d_ytd), sum(d_next_o_id) from district";
    public static final String END_STATE_CUSTOMER = "select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from customer";
    public static final String END_STATE_ORDER = "select max(o_id), sum(o_ol_cnt) from order_new";
    public static final String END_STATE_ORDERLINE = "select sum(ol_amount), sum(ol_quantity) from order_line";
    public static final String END_STATE_STOCK = "select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from stock";
}
