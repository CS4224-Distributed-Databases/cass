package Transactions;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import util.CqlQueries;
import static util.TimeHelper.formatDate;

public class DeliveryTransaction extends BaseTransaction {
    private static final int NUM_DISTRICTS = 10;
    private int warehouseID;
    private int carrierID;

    public DeliveryTransaction(Session session, HashMap<String, PreparedStatement> insertPrepared) {
        super(session, insertPrepared);
    }

    @Override
    public void parseInput(Scanner sc, String inputLine) {
        // Payment expects format of D,W_ID,CARRIER_ID
        String[] input = inputLine.split(",");
        assert(input[0].equals("D"));
        this.warehouseID = Integer.parseInt(input[1]);
        this.carrierID = Integer.parseInt(input[2]);
    }

    @Override
    public void execute() {

        System.out.println("Starting Execution of Delivery Transaction...");
        prepareStatement("YET_DELIVERED_ORDER", CqlQueries.YET_DELIVERED_ORDER);
        prepareStatement("UPDATE_YET_DELIVERED_ORDER", CqlQueries.UPDATE_YET_DELIVERED_ORDER);
        prepareStatement("GET_ORDER_LINES", CqlQueries.GET_ORDER_LINES);
        prepareStatement("UPDATE_ORDER_LINES_DELIVERY_DATE", CqlQueries.UPDATE_ORDER_LINES_DELIVERY_DATE);
        prepareStatement("GET_CUSTOMER_BALANCE_AND_DELIVERY_COUNT", CqlQueries.GET_CUSTOMER_BALANCE_AND_DELIVERY_COUNT);
        prepareStatement("UPDATE_CUSTOMER_BALANCE_AND_DELIVERY_COUNT", CqlQueries.UPDATE_CUSTOMER_BALANCE_AND_DELIVERY_COUNT);

        for (int i = 1; i <= NUM_DISTRICTS; i++) {
            // 1: Find the ID of the oldest yet-to-be-delivered order (from d_next_delivery_o_id).
            List<Row> order = executeQuery("YET_DELIVERED_ORDER", warehouseID, i);

            if(order.size() == 0){
                continue;
            }

            Integer orderID = order.get(0).getInt(CqlQueries.DELIVERY_O_ID_INDEX);
            Integer customerNumber = order.get(0).getInt(CqlQueries.DELIVERY_O_C_ID);

            // 2: Update order O_ID by setting O CARRIER ID to CARRIER ID
            // O_CARRIER_ID, O_ID, O_C_ID, O_W_ID, O_D_ID
            executeQuery("UPDATE_YET_DELIVERED_ORDER", carrierID, customerNumber, orderID, warehouseID, i);

            // 3: Get all the order-lines  by setting OL DELIVERY D to the current date and time
            // OL_W_ID, OL_D_ID, OL_O_ID
            List<Row> allOrderLines = executeQuery("GET_ORDER_LINES", warehouseID, i, orderID);

            BigDecimal orderLineAmount = new BigDecimal(0);
            Date now = new Date();
            Timestamp time = Timestamp.valueOf(formatDate(now));

            for (Row orderLine: allOrderLines){
                // Update DELIVERY_OL_DELIVERY_D to current date and time
                // OL_DELIVERY_D, OL_NUMBER, OL_W_ID, OL_D_ID, OL_O_ID
                executeQuery("UPDATE_ORDER_LINES_DELIVERY_DATE", time, orderLine.getInt(CqlQueries.DELIVERY_OL_NUMBER), warehouseID, i, orderID);
                // Sum the amount from all orderLines
                orderLineAmount = orderLineAmount.add(orderLine.getDecimal(CqlQueries.DELIVERY_OL_AMOUNT));
            }

            // 4: Update balance and delivery count for customer C
            // C_ID, C_W_ID, C_D_ID
            List<Row> customerBalanceAndDeliveryCount = executeQuery("GET_CUSTOMER_BALANCE_AND_DELIVERY_COUNT", customerNumber, warehouseID, i);
            if (customerBalanceAndDeliveryCount.size() == 0){
                continue;
            }
            BigDecimal customerBalance = customerBalanceAndDeliveryCount.get(0).getDecimal(0);
            Integer deliveryCount = customerBalanceAndDeliveryCount.get(0).getInt(1);
            // c_balance, c_delivery_cnt, c_w_id, c_d_id, c_id
            executeQuery("UPDATE_CUSTOMER_BALANCE_AND_DELIVERY_COUNT", orderLineAmount.add(customerBalance), deliveryCount+1, warehouseID, i, customerNumber);
        }

        System.out.println("Finish executing Delivery Transactions...");
    }
}
