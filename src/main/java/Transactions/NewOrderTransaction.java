package Transactions;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import util.CqlQueries;
import util.TimeHelper;

import java.util.*;

public class NewOrderTransaction extends BaseTransaction {
    private int customerWarehouseId;
    private int customerDistrictId;
    private int customerId;
    private int numOfItems;

    private List<Integer> itemNumbers;
    private List<Integer> supplierWarehouses;
    private List<Integer> quantities;

    public NewOrderTransaction(Session session) {
        super(session);
    }

    @Override
    public void parseInput(Scanner sc, String inputLine) {
        // Payment expects format of N,C_ID,W_ID,D_ID,M and has more M lines
        String[] input = inputLine.split(",");
        assert(input[0].equals("N"));
        customerWarehouseId = Integer.parseInt(input[1]);
        customerDistrictId = Integer.parseInt(input[2]);
        customerId = Integer.parseInt(input[3]);
        numOfItems = Integer.parseInt(input[4]);

        itemNumbers = new ArrayList<Integer>();
        supplierWarehouses = new ArrayList<Integer>();
        quantities = new ArrayList<Integer>();
        for(int i = 1; i <= numOfItems; i++) {
            String nextLine = sc.nextLine();
            String[] itemInput =  nextLine.split(",");
            itemNumbers.add(Integer.parseInt(itemInput[0]));
            supplierWarehouses.add(Integer.parseInt(itemInput[1]));
            quantities.add(Integer.parseInt(itemInput[2]));
        }
    }

    @Override
    public void execute() {
        List<String> itemNames = new ArrayList<String>();
        List<Double> orderLineAmounts = new ArrayList<Double>();
        List<Integer> adjustedQuantities = new ArrayList<Integer>();

        // 1. Get and update District Info
        List<Object> args = new ArrayList<Object>(Arrays.asList(customerWarehouseId, customerDistrictId));
        Row districtInfo = executeCqlQuery(CqlQueries.N_GET_DISTRICT_INFO, args).get(0);
        int orderNumber = districtInfo.getInt(CqlQueries.N_D_NEXT_O_ID_INDEX);
        args = new ArrayList<Object>(Arrays.asList(orderNumber + 1, customerWarehouseId, customerDistrictId));
        executeCqlQuery(CqlQueries.N_UPDATE_DISTRICT_NEXT_O_ID, args);

        double totalAmount = 0.0;
        int isAllLocal = 1;

        // 2. Create Order line for each item
        for(int i = 1; i <= numOfItems; i++) {
            int itemNumber = itemNumbers.get(i);
            int supplierWarehouse = supplierWarehouses.get(i);
            int quantity = quantities.get(i);

            // 2.1 get updated quantity and update stock
            String sDistrictNum = "S_DIST_" + customerDistrictId;
            args = new ArrayList<Object>(Arrays.asList(sDistrictNum, supplierWarehouse, itemNumber));
            Row stockInfo = executeCqlQuery(CqlQueries.N_GET_STOCK_INFO, args).get(0);
            int stockQuantity = stockInfo.getInt(CqlQueries.N_S_QUANTITY_INDEX);
            int adjustedQuantity = stockQuantity - quantity;
            if (adjustedQuantity < 10) {
                adjustedQuantity += 100;
            }
            adjustedQuantities.add(adjustedQuantity);

            // TODO: check if converting decimal to double has any loss conversion
            double stockYtd = stockInfo.getDecimal(CqlQueries.N_S_YTD_INDEX).doubleValue();
            int orderCount = stockInfo.getInt(CqlQueries.N_S_ORDER_CNT_INDEX);
            int remoteCount = stockInfo.getInt(CqlQueries.N_S_REMOTE_CNT_INDEX);
            int newRemoteCount = remoteCount;
            if (supplierWarehouse != customerWarehouseId) {
                isAllLocal = 0;
                newRemoteCount += 1;
            }
            args = new ArrayList<Object>(Arrays.asList(adjustedQuantity, stockYtd + 1, orderCount + 1, newRemoteCount, supplierWarehouse, itemNumber));
            executeCqlQuery(CqlQueries.N_UPDATE_STOCK_QUANTITY, args);

            args = new ArrayList<Object>(Arrays.asList(itemNumber));
            Row itemInfo = executeCqlQuery(CqlQueries.N_GET_ITEM_INFO, args).get(0);
            double itemPrice = itemInfo.getDecimal(CqlQueries.N_I_PRICE_INDEX).doubleValue();
            double itemAmount = quantity * itemPrice;
            totalAmount += itemAmount;
            orderLineAmounts.add(itemAmount);
            String itemName = itemInfo.getString(CqlQueries.N_I_NAME_INDEX);
            itemNames.add(itemName);

            // 2.2 create new orderline
            // add I_NAME from customer too
            String sDistInfo = stockInfo.getString(CqlQueries.N_S_DIST_INDEX);
            args = new ArrayList<Object>(Arrays.asList(orderNumber, customerDistrictId, customerWarehouseId, i, itemNumber, supplierWarehouse, quantity, itemAmount, sDistInfo, itemName));
            executeCqlQuery(CqlQueries.N_CREATE_ORDER_LINE, args);
        }

        //3. compute total amount
        double districtTax = districtInfo.getDecimal(CqlQueries.N_D_TAX_INDEX).doubleValue();
        double warehouseTax = districtInfo.getDecimal(CqlQueries.N_W_TAX_INDEX).doubleValue();
        args = new ArrayList<Object>(Arrays.asList(customerWarehouseId, customerDistrictId, customerId));
        Row customerInfo = executeCqlQuery(CqlQueries.N_GET_CUSTOMER_INFO, args).get(0);
        double customerDiscount = customerInfo.getDecimal(CqlQueries.N_C_DISCOUNT_INDEX).doubleValue();
        totalAmount = totalAmount * (1 + districtTax + warehouseTax) * (1 - customerDiscount);
        String customerFirstName = customerInfo.getString(CqlQueries.N_C_FIRST_INDEX);
        String customerMiddleName = customerInfo.getString(CqlQueries.N_C_MIDDLE_INDEX);
        String customerLastName = customerInfo.getString(CqlQueries.N_C_LAST_INDEX);

        //4. Create Order
        // add C_FIRST, C_MIDDLE_C_LAST from customer too
        // brought to the end after creating all order lines to avoid an extra iteration for checking if warehouses are local
        // TODO: check if string to represent timestamp for O_ENTRY_D works
        String entryDate = TimeHelper.formatDate(new Date());
        args = new ArrayList<Object>(Arrays.asList(orderNumber, customerDistrictId, customerWarehouseId, customerId, entryDate, numOfItems, isAllLocal, customerFirstName, customerMiddleName, customerLastName));
        executeCqlQuery(CqlQueries.N_CREATE_ORDER, args);

        //4. Print output
        System.out.println(String.format(
                "1. Customer: (%d, %d, %d), C_LAST: %s, C_CREDIT: %s, C_DISCOUNT: %.4f",
                customerWarehouseId, customerDistrictId, customerId,
                customerLastName, customerInfo.getString(CqlQueries.N_C_CREDIT_INDEX), customerDiscount));
        System.out.println(String.format("2. W_TAX: %.4f, D_TAX: %.4f", warehouseTax, districtTax));
        System.out.println(String.format("3. O_ID: %d, O_ENTRY_D: %s", orderNumber, entryDate));
        System.out.println(String.format("4. NUM_ITEMS: %d, TOTAL_AMOUNT: %.2f", numOfItems, totalAmount));
        System.out.println("5. Each item details:");
        for(int i = 0; i < numOfItems; i++) {
            System.out.println(String.format(
                    "\t ITEM_NUMBER: %d, I_NAME: %s, SUPPLIER_WAREHOUSE: %d, QUANTITY: %d, OL_AMOUNT: %.2f, S_QUANTITY: %d",
                    itemNumbers.get(i), itemNames.get(i), supplierWarehouses.get(i), quantities.get(i), orderLineAmounts.get(i), adjustedQuantities.get(i)));
        }
    }
}
