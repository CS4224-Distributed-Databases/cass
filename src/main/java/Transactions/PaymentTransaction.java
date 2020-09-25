package Transactions;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import util.CqlQueries;
import util.TimeHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PaymentTransaction extends BaseTransaction {
    private int customerWarehouseId;
    private int customerDistrictId;
    private int customerId;
    private double payment;

    public PaymentTransaction(Session session) {
        super(session);
    }

    @Override
    public void parseInput(String[] input) {
        // Payment expects format of P,C W ID,C D ID,C ID,PAYMENT.
        assert(input[0].equals("P"));
        customerWarehouseId = Integer.parseInt(input[1]);
        customerDistrictId = Integer.parseInt(input[2]);
        customerId = Integer.parseInt(input[3]);
        payment = Double.parseDouble(input[4]);
    }

    @Override
    public void execute() {
        // 1. Get and Update Warehouse info
        List<Object> args = new ArrayList<Object>(Arrays.asList(customerWarehouseId));
        Row warehouseInfo = executeCqlQuery(CqlQueries.GET_WAREHOUSE_INFO, args).get(0);
        double warehousePayment = warehouseInfo.getDecimal(CqlQueries.PAYMENT_W_YTD_INDEX).doubleValue();
        args = new ArrayList<Object>(Arrays.asList(warehousePayment + payment, customerWarehouseId));
        executeCqlQuery(CqlQueries.UPDATE_WAREHOUSE_PAYMENT, args);

        // 2. Get and Update District info
        args = new ArrayList<Object>(Arrays.asList(customerWarehouseId, customerDistrictId));
        Row districtInfo = executeCqlQuery(CqlQueries.GET_DISTRICT_INFO, args).get(0);
        double districtPayment = districtInfo.getDecimal(CqlQueries.PAYMENT_D_YTD_INDEX).doubleValue();
        args = new ArrayList<Object>(Arrays.asList(districtPayment + payment, customerWarehouseId, customerDistrictId));
        executeCqlQuery(CqlQueries.UPDATE_DISTRICT_PAYMENT, args);

        // 3. Get and Update Customer info
        args = new ArrayList<Object>(Arrays.asList(customerWarehouseId, customerDistrictId, customerId));
        Row customerInfo = executeCqlQuery(CqlQueries.GET_CUSTOMER_INFO, args).get(0);
        double customerBalance = customerInfo.getDecimal(CqlQueries.PAYMENT_C_BALANCE_INDEX).doubleValue();
        float customerPayment = customerInfo.getFloat(CqlQueries.PAYMENT_C_YTD_PAYMENT_INDEX);
        int customerPaymentCount = customerInfo.getInt(CqlQueries.PAYMENT_C_PAYMENT_CNT_INDEX);
        args = new ArrayList<Object>(Arrays.asList(
                customerBalance - payment,
                customerPayment + payment,
                customerPaymentCount + 1,
                customerWarehouseId, customerDistrictId, customerId));
        executeCqlQuery(CqlQueries.UPDATE_CUSTOMER_PAYMENT, args);

        //4. Print output
        //TODO: check if this is the correct format expected
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
                customerBalance - payment
        ));

        System.out.println(String.format(
                "2. Warehouse Address: (%s, %s, %s, %s, %s)",
                customerInfo.getString(CqlQueries.PAYMENT_W_STREET_1_INDEX), customerInfo.getString(CqlQueries.PAYMENT_W_STREET_2_INDEX), customerInfo.getString(CqlQueries.PAYMENT_W_CITY_INDEX),
                customerInfo.getString(CqlQueries.PAYMENT_W_STATE_INDEX), customerInfo.getString(CqlQueries.PAYMENT_W_ZIP_INDEX)
        ));

        System.out.println(String.format(
                "3. District Address: (%s, %s, %s, %s, %s)",
                customerInfo.getString(CqlQueries.PAYMENT_D_STREET_1_INDEX), customerInfo.getString(CqlQueries.PAYMENT_D_STREET_2_INDEX), customerInfo.getString(CqlQueries.PAYMENT_D_CITY_INDEX),
                customerInfo.getString(CqlQueries.PAYMENT_D_STATE_INDEX), customerInfo.getString(CqlQueries.PAYMENT_D_ZIP_INDEX)
        ));

        System.out.println(String.format("4. Payment: %.2f", payment));
    }
}
