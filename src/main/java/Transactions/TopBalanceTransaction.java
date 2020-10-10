package Transactions;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import util.CqlQueries;
import util.CustomerData;

import java.math.BigDecimal;
import java.util.*;


public class TopBalanceTransaction extends BaseTransaction {
    private static final int count = 10;

    public TopBalanceTransaction(Session session, HashMap<String, PreparedStatement> insertPrepared, String consistencyType) {
        super(session, insertPrepared, consistencyType);
    }

    @Override
    public void parseInput(Scanner sc, String inputLine) {
        assert(inputLine.equals("T"));
    }

    @Override
    public void execute() {

        System.out.println("Starting Execution of Top Balance Transaction...");
        prepareStatement("GET_CUSTOMERS_ORDERED_BY_BALANCE", CqlQueries.GET_CUSTOMERS_ORDERED_BY_BALANCE);

        // Algorithm:
        // Get the top 10 customers from each (warehouseId, districtId) pair
        // Add into minheap and maintain it of size 10 (keep track of the top 10 highest balance across all customers)
        // if a next value to add into pq > minValue in pq, then remove this pq minValue and add in this new larger value
        // TODO: check if its okay to hardcode the warehouse and district ids? Or need to retrieve them from tables?

        PriorityQueue<CustomerData> pq = new PriorityQueue<>();

        for (int warehouseId = 1; warehouseId <= 10; warehouseId++) {
            for (int districtId = 1; districtId <= 10; districtId++) {
                List<Row> topCustomersPerPair = executeQuery("GET_CUSTOMERS_ORDERED_BY_BALANCE", warehouseId, districtId);
                for (Row customerPerPair: topCustomersPerPair) {
                    BigDecimal newCustomerBalance = customerPerPair.getDecimal(CqlQueries.T_C_BALANCE_INDEX);

                    // add to pq only size < 10 or the new balance value is larger than the smallest value in the pq
                    if (pq.size() < count) {
                        CustomerData newCustomer = new CustomerData(warehouseId, districtId, customerPerPair.getInt(CqlQueries.T_C_ID_INDEX),
                                newCustomerBalance, customerPerPair.getString(CqlQueries.T_C_FIRST_INDEX),
                                customerPerPair.getString(CqlQueries.T_C_MIDDLE_INDEX), customerPerPair.getString(CqlQueries.T_C_LAST_INDEX),
                                customerPerPair.getString(CqlQueries.T_C_W_NAME_INDEX), customerPerPair.getString(CqlQueries.T_C_D_NAME_INDEX));
                        pq.add(newCustomer);
                    } else {
                        CustomerData current10thCustomer = pq.peek();
                        if (current10thCustomer.balance.compareTo(newCustomerBalance) < 0) {
                            pq.poll();
                            CustomerData newCustomer = new CustomerData(warehouseId, districtId, customerPerPair.getInt(CqlQueries.T_C_ID_INDEX),
                                    newCustomerBalance, customerPerPair.getString(CqlQueries.T_C_FIRST_INDEX),
                                    customerPerPair.getString(CqlQueries.T_C_MIDDLE_INDEX), customerPerPair.getString(CqlQueries.T_C_LAST_INDEX),
                                    customerPerPair.getString(CqlQueries.T_C_W_NAME_INDEX), customerPerPair.getString(CqlQueries.T_C_D_NAME_INDEX));
                            pq.add(newCustomer);
                        }
                    }
                }
            }
        }

        // Store the top 10 in an array list in ascending order and iterate from the back
        List<CustomerData> top10Customers = new ArrayList<>();
        while (pq.size() > 0) {
            top10Customers.add(pq.poll());
        }

        for (int i = 9; i >= 0; i--) {
            CustomerData topCustomer = top10Customers.get(i);
            System.out.println("Customer "+ (10-i));
            System.out.println(String.format("1. Customer Name: %s, %s, %s", topCustomer.firstName, topCustomer.middleName, topCustomer.lastName));
            System.out.println(String.format("2. Outstanding balance: %.2f", topCustomer.balance));
            System.out.println(String.format("3. Warehouse Name: %s", topCustomer.warehouseName));
            System.out.println(String.format("4. District Name: %s", topCustomer.districtName));
        }

        System.out.println("Finish executing Top Balance Transactions...");
    }
}