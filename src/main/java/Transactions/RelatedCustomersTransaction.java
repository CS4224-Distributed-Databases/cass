package Transactions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

import com.datastax.driver.core.PreparedStatement;
import util.CqlQueries;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.reflect.TypeToken;


public class RelatedCustomersTransaction extends BaseTransaction {
    private static final int NUM_DISTRICTS = 10;
    private int warehouseID;
    private int districtID;
    private int customerID;

    public RelatedCustomersTransaction(Session session, HashMap<String, PreparedStatement> insertPrepared) {
        super(session, insertPrepared);
    }

    @Override
    public void parseInput(Scanner sc, String inputLine) {
        // Related Customers expects format of R, W_ID, D_ID, C_ID
        String[] input = inputLine.split(",");
        assert(input[0].equals("R"));
        this.warehouseID = Integer.parseInt(input[1]);
        this.districtID = Integer.parseInt(input[2]);
        this.customerID = Integer.parseInt(input[3]);
    }


    @Override
    public void execute() {

        System.out.println("Starting Execution of Related Customers Transaction...");

        prepareStatement("GET_CUSTOMER_ORDERS", CqlQueries.GET_CUSTOMER_ORDERS);
        prepareStatement("GET_ITEM_NUMBER_FROM_ORDER_LINE", CqlQueries.GET_ITEM_NUMBER_FROM_ORDER_LINE);
        prepareStatement("GET_CUS_LIST_FROM_ITEM", CqlQueries.GET_CUS_LIST_FROM_ITEM);

        // 1: Get the orders for the corresponding input given
        // O_W_ID, O_D_ID, O_C_ID
        Integer orderID = executeQuery("GET_CUSTOMER_ORDERS", this.warehouseID, this.districtID, this.customerID).get(0).getInt(0);

        // 2: Get all the items in the Order_line related to this order
        // OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID
        List<Row> allItemNumbers = executeQuery("GET_ITEM_NUMBER_FROM_ORDER_LINE", this.warehouseID, this.districtID, orderID);


        // 3: For each item, search through the customers
        // and check if they have appeared 2x
        HashSet<String> customersSeenBefore = new HashSet<>();
        HashSet<String> relatedCustomers = new HashSet<>();
        for(Row item: allItemNumbers){
            // get the list of customers associated with this item
            // I_ID
            List<Row> allCustomers = executeQuery("GET_CUS_LIST_FROM_ITEM", item.getInt(0));

            for (Row cus: allCustomers){

                String customerIdentifierString = cus.getList(0, TypeToken.of(String.class)).get(0);
                String[] customerIdentifier = customerIdentifierString.split(" "); // O_W_ID, O_D_ID, O_C_ID

                String O_W_ID = customerIdentifier[0];

                // Check if this customer belongs to a different warehouse
                if (Integer.parseInt(O_W_ID) == this.warehouseID) {
                    continue;
                }

                // Check if it has appeared in customersSeenBefore Hashset before. If it has, then it means this current customer
                // has >= 2 common items with the current customer and we can add to our final output
                // Otherwise, we add it to customersSeenBefore Hashset
                if (!customersSeenBefore.contains(customerIdentifierString)) {
                    customersSeenBefore.add(customerIdentifierString);
                } else {
                    relatedCustomers.add(customerIdentifierString);
                }
            }
        }

        System.out.println("Below are the Related Customers....");
        for (String customer: relatedCustomers){
            System.out.println(customer);
        }

        System.out.println("Finish executing Related Customers Transactions...");
    }
}
