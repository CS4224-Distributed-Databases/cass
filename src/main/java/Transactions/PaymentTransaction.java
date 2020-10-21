package Transactions;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import util.CqlQueries;
import util.TimeHelper;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Scanner;

public class PaymentTransaction extends BaseTransaction {
    private int customerWarehouseId;
    private int customerDistrictId;
    private int customerId;
    private BigDecimal payment;

    public PaymentTransaction(Session session, HashMap<String, PreparedStatement> insertPrepared, String consistencyType) {
        super(session, insertPrepared, consistencyType);
    }

    @Override
    public void parseInput(Scanner sc, String inputLine) {
        // Payment expects format of P,C W ID,C D ID,C ID,PAYMENT.
        String[] input = inputLine.split(",");
        assert(input[0].equals("P"));
        customerWarehouseId = Integer.parseInt(input[1]);
        customerDistrictId = Integer.parseInt(input[2]);
        customerId = Integer.parseInt(input[3]);
        payment = new BigDecimal(input[4]);
    }

    @Override
    public void execute() {

        System.out.println("Start Payment...");
        prepareStatement("P_GET_WAREHOUSE_INFO", CqlQueries.P_GET_WAREHOUSE_INFO);
        prepareStatement("P_UPDATE_WAREHOUSE_PAYMENT", CqlQueries.P_UPDATE_WAREHOUSE_PAYMENT);
        prepareStatement("P_GET_DISTRICT_INFO", CqlQueries.P_GET_DISTRICT_INFO);
        prepareStatement("P_UPDATE_DISTRICT_PAYMENT", CqlQueries.P_UPDATE_DISTRICT_PAYMENT);
        prepareStatement("P_GET_CUSTOMER_INFO", CqlQueries.P_GET_CUSTOMER_INFO);
        prepareStatement("P_UPDATE_CUSTOMER_PAYMENT", CqlQueries.P_UPDATE_CUSTOMER_PAYMENT);

//        System.out.println(customerWarehouseId + " "+ customerDistrictId + " "+ customerId + " " + payment);

        // 1. Get and Update Warehouse info
        Row warehouseInfo = executeQuery("P_GET_WAREHOUSE_INFO", customerWarehouseId).get(0);
        BigDecimal warehousePayment = warehouseInfo.getDecimal(CqlQueries.PAYMENT_W_YTD_INDEX);
        executeQuery("P_UPDATE_WAREHOUSE_PAYMENT", warehousePayment.add(payment), customerWarehouseId);

        // 2. Get and Update District info
        Row districtInfo = executeQuery("P_GET_DISTRICT_INFO", customerWarehouseId, customerDistrictId).get(0);
        BigDecimal districtPayment = districtInfo.getDecimal(CqlQueries.PAYMENT_D_YTD_INDEX);
        executeQuery("P_UPDATE_DISTRICT_PAYMENT", districtPayment.add(payment), customerWarehouseId, customerDistrictId);

        // 3. Get and Update Customer info
        Row customerInfo = executeQuery("P_GET_CUSTOMER_INFO", customerWarehouseId, customerDistrictId, customerId).get(0);
        BigDecimal customerBalance = customerInfo.getDecimal(CqlQueries.PAYMENT_C_BALANCE_INDEX);
        float customerPayment = customerInfo.getFloat(CqlQueries.PAYMENT_C_YTD_PAYMENT_INDEX);
        int customerPaymentCount = customerInfo.getInt(CqlQueries.PAYMENT_C_PAYMENT_CNT_INDEX);
        executeQuery("P_UPDATE_CUSTOMER_PAYMENT",
                customerBalance.subtract(payment),
                customerPayment + payment.floatValue(),
                customerPaymentCount + 1,
                customerWarehouseId, customerDistrictId, customerId);

        //4. Print output
        System.out.println(String.format(
                "1. Customer: (%d, %d, %d), Name: (%s, %s, %s), Address: (%s, %s, %s, %s, %s), C_PHONE: %s, C_SINCE: %s, C_CREDIT: %s, C_CREDIT_LIM: %.2f, C_DISCOUNT: %.4f, C_BALANCE: %.2f",
                customerWarehouseId, customerDistrictId, customerId,
                customerInfo.getString(CqlQueries.PAYMENT_C_FIRST_INDEX), customerInfo.getString(CqlQueries.PAYMENT_C_MIDDLE_INDEX), customerInfo.getString(CqlQueries.PAYMENT_C_LAST_INDEX),
                customerInfo.getString(CqlQueries.PAYMENT_C_STREET_1_INDEX), customerInfo.getString(CqlQueries.PAYMENT_C_STREET_2_INDEX), customerInfo.getString(CqlQueries.PAYMENT_C_CITY_INDEX), customerInfo.getString(CqlQueries.PAYMENT_C_STATE_INDEX), customerInfo.getString(CqlQueries.PAYMENT_C_ZIP_INDEX),
                customerInfo.getString(CqlQueries.PAYMENT_C_PHONE_INDEX),
                TimeHelper.formatDate(customerInfo.getTimestamp(CqlQueries.PAYMENT_C_SINCE_INDEX)),
                customerInfo.getString(CqlQueries.PAYMENT_C_CREDIT_INDEX),
                customerInfo.getDecimal(CqlQueries.PAYMENT_C_CREDIT_LIM_INDEX).doubleValue(),
                customerInfo.getDecimal(CqlQueries.PAYMENT_C_DISCOUNT_INDEX).doubleValue(),
                customerBalance.subtract(payment)
        ));

        System.out.println(String.format(
                "2. Warehouse Address: (%s, %s, %s, %s, %s)",
                warehouseInfo.getString(CqlQueries.PAYMENT_W_STREET_1_INDEX), warehouseInfo.getString(CqlQueries.PAYMENT_W_STREET_2_INDEX), warehouseInfo.getString(CqlQueries.PAYMENT_W_CITY_INDEX),
                warehouseInfo.getString(CqlQueries.PAYMENT_W_STATE_INDEX), warehouseInfo.getString(CqlQueries.PAYMENT_W_ZIP_INDEX)
        ));

        System.out.println(String.format(
                "3. District Address: (%s, %s, %s, %s, %s)",
                districtInfo.getString(CqlQueries.PAYMENT_D_STREET_1_INDEX), districtInfo.getString(CqlQueries.PAYMENT_D_STREET_2_INDEX), districtInfo.getString(CqlQueries.PAYMENT_D_CITY_INDEX),
                districtInfo.getString(CqlQueries.PAYMENT_D_STATE_INDEX), districtInfo.getString(CqlQueries.PAYMENT_D_ZIP_INDEX)
        ));

        System.out.println(String.format("4. Payment: %.2f", payment));
        System.out.println("Finish Payment...");
    }
}
