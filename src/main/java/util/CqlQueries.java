package util;

// Note: Cassandra will order the partition keys and the clustering keys (ordered by their precedence in the
// PRIMARY KEY definition), and then the columns follow in ascending order.
// Hence the ordering of the columns in createTable is not followed.
// Ensure that the indexes constants created below follow the correct one.

public class CqlQueries {

    // -------------PAYMENT TRANSACTION ----------------------------------------------------------------------------

    public static final String GET_WAREHOUSE_INFO = "SELECT W_YTD, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP " +
            "FROM warehouse WHERE W_ID = ?";
    public static final String UPDATE_WAREHOUSE_PAYMENT = "UPDATE warehouse SET W_YTD = ? WHERE W_ID = ?";
    public static final String GET_DISTRICT_INFO = "SELECT D_YTD, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP" +
            "FROM district WHERE D_W_ID = ? AND D_ID = ?";
    public static final String UPDATE_DISTRICT_PAYMENT = "UPDATE district SET D_YTD = ? WHERE D_W_ID = ? AND D_ID = ?";
    public static final String GET_CUSTOMER_INFO = "SELECT C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, " +
            "C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP " +
            "C_PHONE, C_SINCE, C_CREDIT, C_CREDIT LIM, C_DISCOUNT FROM customer " +
            "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";
    public static final String UPDATE_CUSTOMER_PAYMENT = "UPDATE customer SET C_BALANCE = ? AND C_YTD_PAYMENT = ? " +
            "AND C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";

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

    // -------------DELIVERY TRANSACTION ----------------------------------------------------------------------------
    public static String YET_DELIVERED_ORDER
            = "SELECT * FROM Order_New WHERE O_W_ID = ? AND O_D_ID = ? LIMIT 1;";
    public static final String UPDATE_YET_DELIVERED_ORDER
            = "UPDATE Order_New SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_W_ID = ? AND O_D_ID = ? IF EXISTS;";
    public static final String GET_ORDER_LINES = "SELECT * FROM Order_Line WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?";
    public static final String UPDATE_ORDER_LINES_DELIVERY_DATE
            = "UPDATE Order_Line SET OL_DELIVERY_D = ? WHERE OL_NUMBER = ? AND OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?";
    public static final String GET_CUSTOMER_BALANCE_AND_DELIVERY_COUNT
            = "SELECT C_BALANCE, C_DELIVERY_CNT FROM CUSTOMER WHERE C_ID = ? AND C_W_ID = ? AND C_D_ID = ?";
    public static final String UPDATE_CUSTOMER_BALANCE_AND_DELIVERY_COUNT
            = "UPDATE CUSTOMER SET c_balance = ?, c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";

    // Indexes for Delivery Transaction
    public static final int DELIVERY_O_ID_INDEX = 2;
    public static final int DELIVERY_O_C_ID = 5;

    public static final int DELIVERY_OL_NUMBER = 3;
    public static final int DELIVERY_OL_AMOUNT = 4;

}
