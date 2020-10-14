package Transactions;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import util.CqlQueries;
import util.TimeHelper;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class OrderStatusTransaction extends BaseTransaction {
    private int customerWarehouseId;
    private int customerDistrictId;
    private int customerId;

    public OrderStatusTransaction(Session session, HashMap<String, PreparedStatement> insertPrepared, String consistencyType) {
        super(session, insertPrepared, consistencyType);
    }

    @Override
    public void parseInput(Scanner sc, String inputLine) {
        // Payment expects format of O,C W ID,C D ID,C ID.
        String[] input = inputLine.split(",");
        assert(input[0].equals("O"));
        customerWarehouseId = Integer.parseInt(input[1]);
        customerDistrictId = Integer.parseInt(input[2]);
        customerId = Integer.parseInt(input[3]);
    }

    @Override
    public void execute() {

        System.out.println("Starting Execution of Order Status Transaction...");
        prepareStatement("O_GET_CUSTOMER_INFO", CqlQueries.O_GET_CUSTOMER_INFO);
        prepareStatement("O_GET_CUSTOMER_LAST_ORDER", CqlQueries.O_GET_CUSTOMER_LAST_ORDER);
        prepareStatement("O_GET_LAST_ORDER_DETAILS", CqlQueries.O_GET_LAST_ORDER_DETAILS);


        Row customerInfo = executeQuery("O_GET_CUSTOMER_INFO", customerWarehouseId, customerDistrictId, customerId).get(0);
        String customerFirstName = customerInfo.getString(CqlQueries.ORDER_C_FIRST_INDEX);
        String customerMiddleName = customerInfo.getString(CqlQueries.ORDER_C_MIDDLE_INDEX);
        String customerLastName = customerInfo.getString(CqlQueries.ORDER_C_LAST_INDEX);
        BigDecimal customerBalance = customerInfo.getDecimal(CqlQueries.ORDER_C_BALANCE_INDEX);

        System.out.printf("Customer name: %s %s %s, balance: %f\n",
                customerFirstName,
                customerMiddleName,
                customerLastName,
                customerBalance);

        Row lastOrder = executeQuery("O_GET_CUSTOMER_LAST_ORDER", customerWarehouseId, customerDistrictId, customerId).get(0);
        Integer lastOrderID = lastOrder.getInt(CqlQueries.ORDER_O_ORDER_ID_INDEX);
        Integer carrierID = lastOrder.getInt(CqlQueries.ORDER_O_CARRIER_ID_INDEX);
        String entryDate = TimeHelper.formatDate(lastOrder.getTimestamp(CqlQueries.ORDER_O_ENTRY_INDEX));
        System.out.printf("Customer's last order ID: %d, entry time: %s, carrier ID: %d\n",
                lastOrderID,
                entryDate,
                carrierID);

        List<Row> lastOrderDetails = executeQuery("O_GET_LAST_ORDER_DETAILS", customerWarehouseId, customerDistrictId, lastOrderID);
        for (Row item: lastOrderDetails){
            Integer itemID = item.getInt(CqlQueries.ORDER_OL_ITEM_ID_INDEX);
            Integer supplyWarehouse = item.getInt(CqlQueries.ORDER_OL_SUPPLY_WAREHOUSE_ID_INDEX);
            BigDecimal quantity = item.getDecimal(CqlQueries.ORDER_OL_QUANTITY_INDEX);
            BigDecimal amount = item.getDecimal(CqlQueries.ORDER_OL_AMOUNT_INDEX);
            Date deliveryDate = item.getTimestamp(CqlQueries.ORDER_OL_DELIVERY_DATE_INDEX);
            String deliveryDateString = "";
            if (deliveryDate != null) {
                deliveryDateString = TimeHelper.formatDate(deliveryDate);
            }

            System.out.printf("Order line in last order item ID: %d, supply warehouse ID: %d, "
                            + "quantity: %f, price: %f, delivery date: %s\n",
                    itemID,
                    supplyWarehouse,
                    quantity,
                    amount,
                    deliveryDateString);
        }
        System.out.println("Finish executing Order Status Transaction...");
    }
}
//O,1,1,95