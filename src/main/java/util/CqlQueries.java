package util;

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
            = "SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = ? AND dt_d_id = ?";
    public static final String UPDATE_YET_DELIVERED_ORDER
            = "UPDATE delivery_transaction SET dt_min_ud_o_id = dt_min_ud_o_id + ? WHERE dt_w_id = ? AND dt_d_id = ?";
    public static final String GET_CUSTOMER_ID_FROM_ORDER
            = "SELECT o_c_id FROM order_by_o_id WHERE o_w_id = ? AND o_d_id = ? and o_id = ?";
    public static final String UPDATE_ORDER_CARRIER_ID =
            "UPDATE order_by_o_id SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? and o_id = ?";
    public static final String GET_ORDER_LINES = "SELECT ol_number, ol_amount FROM order_line_item WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?";
    public static final String UPDATE_ORDER_LINES_DELIVERY_DATE
            = "UPDATE order_line_item SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? and ol_number = ?";
    public static final String GET_CUSTOMER_BALANCE_AND_DELIVERY_COUNT
            = "SELECT c_balance, c_delivery_cnt FROM customer_stats WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
    public static final String UPDATE_CUSTOMER_BALANCE_AND_DELIVERY_COUNT
            = "UPDATE customer_stats SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";

    public static int INDEX_MIN_UNDELIVERED_ORDER_ID = 0;

    public static int INDEX_CUSTOMER_ID = 0;

    public static int INDEX_ORDER_LINE_NUMBER = 0;
    public static int INDEX_ORDER_LINE_AMOUNT = 1;

    public static int INDEX_CUSTOMER_BALANCE = 0;
}
