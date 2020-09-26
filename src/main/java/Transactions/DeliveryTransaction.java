package Transactions;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import util.CqlQueries;
import util.TimeHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class DeliveryTransaction extends BaseTransaction {
    private static final int NUM_DISTRICTS = 10;
    private int warehouseID;
    private int carrierID;


    public DeliveryTransaction(Session session) {
        super(session);
    }

    @Override
    public void parseInput(String[] input) {
        // Payment expects format of D,W_ID,CARRIER_ID
        assert(input[0].equals("D"));
        warehouseID = Integer.parseInt(input[1]);
        carrierID = Integer.parseInt(input[2]);
    }

    @Override
    public void execute() {
        for (int i = 1; i <= NUM_DISTRICTS; i++) {
            // Find the ID of the oldest yet-to-be-delivered order (from d_next_delivery_o_id).
            List<Object> args = new ArrayList<Object>(Arrays.asList(warehouseID, i));
            long orderID = executeCqlQuery(CqlQueries.YET_DELIVERED_ORDER, args).get(0).getLong("d_next_delivery_o_id");
            System.out.printf("The oldest yet-to-be-delivered order in warehouse %d district %d is %d.\n",
                    warehouseID, i, orderID);

            // Updates the ID of the oldest yet-to-be-delivered order.
            executeCqlQuery(CqlQueries.UPDATE_YET_DELIVERED_ORDER, args);

            // get order lines
            List<Object> getOrderArgs = new ArrayList<Object>(Arrays.asList(warehouseID, i, orderID));
            List<Row> orderLines = executeCqlQuery(CqlQueries.GET_ORDER_LINES, getOrderArgs);
            double orderLineAmount = 0;
            Date now = new Date();

            // update order lines with current date time
            for(Row orderLine : orderLines) {
                orderLineAmount += orderLine.getDouble(CqlQueries.INDEX_ORDER_LINE_AMOUNT);
                int orderLineNumber = orderLine.getInt(CqlQueries.INDEX_ORDER_LINE_NUMBER);
                List<Object> updateOrderLineArgs = new ArrayList<Object>(Arrays.asList(now, warehouseID, i, orderID, orderLineNumber));
                executeCqlQuery(CqlQueries.UPDATE_ORDER_LINES_DELIVERY_DATE, updateOrderLineArgs);
            }

            // update order with carrier id
            List<Object> updateCarrierOrderIdArgs = new ArrayList<Object>(Arrays.asList(carrierID, warehouseID, i, orderID));
            executeCqlQuery(CqlQueries.UPDATE_ORDER_CARRIER_ID, updateCarrierOrderIdArgs);

            // get customer id from order
            List<Object> getCustomerIdArgs = new ArrayList<Object>(Arrays.asList(warehouseID, i, i, orderID));
            Row customerRow = executeCqlQuery(CqlQueries.GET_CUSTOMER_ID_FROM_ORDER, getCustomerIdArgs).get(0);
            int customerId = customerRow.getInt(CqlQueries.INDEX_CUSTOMER_ID);

            // update customer balance and delivery count
            double totalOrderLineAmount = orderLineAmount;
            List<Object> updateCustomerArgs = new ArrayList<Object>(Arrays.asList((long)(totalOrderLineAmount * 100), 1L,
                    warehouseID, i, customerId));
            executeCqlQuery(CqlQueries.UPDATE_CUSTOMER_BALANCE_AND_DELIVERY_COUNT, updateCustomerArgs);
        }
    }
}
