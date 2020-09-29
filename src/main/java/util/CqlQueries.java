package util;

public class CqlQueries {

    // -------------PAYMENT TRANSACTION ----------------------------------------------------------------------------

    public static final String P_GET_WAREHOUSE_INFO = "SELECT W_YTD, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP " +
            "FROM Warehouse WHERE W_ID = ?";
    public static final String P_UPDATE_WAREHOUSE_PAYMENT = "UPDATE Warehouse SET W_YTD = ? WHERE W_ID = ?";
    public static final String P_GET_DISTRICT_INFO = "SELECT D_YTD, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP" +
            "FROM District WHERE D_W_ID = ? AND D_ID = ?";
    public static final String P_UPDATE_DISTRICT_PAYMENT = "UPDATE District SET D_YTD = ? WHERE D_W_ID = ? AND D_ID = ?";
    public static final String P_GET_CUSTOMER_INFO = "SELECT C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, " +
            "C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP " +
            "C_PHONE, C_SINCE, C_CREDIT, C_CREDIT LIM, C_DISCOUNT FROM Customer " +
            "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";
    public static final String P_UPDATE_CUSTOMER_PAYMENT = "UPDATE Customer SET C_BALANCE = ?, C_YTD_PAYMENT = ?, " +
            "C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";

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




}
