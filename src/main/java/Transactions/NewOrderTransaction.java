package Transactions;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import util.CqlQueries;
import util.TimeHelper;

import java.math.BigDecimal;
import java.util.*;

public class NewOrderTransaction extends BaseTransaction {
    private int customerWarehouseId;
    private int customerDistrictId;
    private int customerId;
    private int numOfItems;

    private List<Integer> itemNumbers;
    private List<Integer> supplierWarehouses;
    private List<Integer> quantities;

    public NewOrderTransaction(Session session, HashMap<String, PreparedStatement> insertPrepared) {
        super(session, insertPrepared);
    }

    @Override
    public void parseInput(Scanner sc, String inputLine) {
        // Payment expects format of N,C_ID,W_ID,D_ID,M and has more M lines
        String[] input = inputLine.split(",");
        assert(input[0].equals("N"));
        customerId = Integer.parseInt(input[1]);
        customerWarehouseId = Integer.parseInt(input[2]);
        customerDistrictId = Integer.parseInt(input[3]);
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

        System.out.println("Starting Execution of New Order Transaction...");
        prepareStatement("N_GET_DISTRICT_INFO", CqlQueries.N_GET_DISTRICT_INFO);
        prepareStatement("N_UPDATE_DISTRICT_NEXT_O_ID", CqlQueries.N_UPDATE_DISTRICT_NEXT_O_ID);
        prepareStatement("N_GET_CUSTOMER_INFO", CqlQueries.N_GET_CUSTOMER_INFO);
        prepareStatement("N_GET_STOCK_INFO", CqlQueries.N_GET_STOCK_INFO);
        prepareStatement("N_UPDATE_STOCK_QUANTITY", CqlQueries.N_UPDATE_STOCK_QUANTITY);
        prepareStatement("N_GET_ITEM_INFO", CqlQueries.N_GET_ITEM_INFO);
        prepareStatement("N_CREATE_ORDER_LINE", CqlQueries.N_CREATE_ORDER_LINE);
        prepareStatement("N_CREATE_ORDER", CqlQueries.N_CREATE_ORDER);
        prepareStatement("N_CREATE_ORDER_SMALL", CqlQueries.N_CREATE_ORDER_SMALL);
        prepareStatement("N_UPDATE_ITEM_CUSTOMER_LIST", CqlQueries.N_UPDATE_ITEM_CUSTOMER_LIST);

        List<String> itemNames = new ArrayList<String>();
        List<BigDecimal> orderLineAmounts = new ArrayList<BigDecimal>();
        List<Integer> adjustedQuantities = new ArrayList<Integer>();

        // 1. Get and update District Info
        Row districtInfo = executeQuery("N_GET_DISTRICT_INFO", customerWarehouseId, customerDistrictId).get(0);
        int orderNumber = districtInfo.getInt(CqlQueries.N_D_NEXT_O_ID_INDEX);
        executeQuery("N_UPDATE_DISTRICT_NEXT_O_ID", orderNumber + 1, customerWarehouseId, customerDistrictId);

        BigDecimal totalAmount = new BigDecimal(0);
        int isAllLocal = 1;

        // 2. Create Order line for each item
        for(int i = 1; i <= numOfItems; i++) {
            int itemNumber = itemNumbers.get(i);
            int supplierWarehouse = supplierWarehouses.get(i);
            int quantity = quantities.get(i);

            // 2.1 get updated quantity and update stock
            String zeroAppendedDistrictId = (customerDistrictId%10 == 0) ? String.valueOf(customerDistrictId) : "0" + customerDistrictId;
            String sDistrictNum = "S_DIST_" + zeroAppendedDistrictId;
            Row stockInfo = executeQuery("N_GET_STOCK_INFO", supplierWarehouse, itemNumber).get(0);
            int stockQuantity = stockInfo.getInt("S_QUANTITY");
            int adjustedQuantity = stockQuantity - quantity;
            if (adjustedQuantity < 10) {
                adjustedQuantity += 100;
            }
            adjustedQuantities.add(adjustedQuantity);

            BigDecimal stockYtd = stockInfo.getDecimal("S_YTD");
            int orderCount = stockInfo.getInt("S_ORDER_CNT");
            int remoteCount = stockInfo.getInt("S_REMOTE_CNT");
            int newRemoteCount = remoteCount;
            if (supplierWarehouse != customerWarehouseId) {
                isAllLocal = 0;
                newRemoteCount += 1;
            }
            executeQuery("N_UPDATE_STOCK_QUANTITY",
                    adjustedQuantity, stockYtd.add(BigDecimal.ONE), orderCount + 1, newRemoteCount, supplierWarehouse, itemNumber);
            Row itemInfo = executeQuery("N_GET_ITEM_INFO", itemNumber).get(0);
            BigDecimal itemPrice = itemInfo.getDecimal(CqlQueries.N_I_PRICE_INDEX);
            BigDecimal itemAmount = itemPrice.multiply(new BigDecimal(quantity));
            totalAmount = totalAmount.add(itemAmount);
            orderLineAmounts.add(itemAmount);
            String itemName = itemInfo.getString(CqlQueries.N_I_NAME_INDEX);
            itemNames.add(itemName);

            // 2.2 create new orderline
            // add I_NAME from customer too
            String sDistInfo = stockInfo.getString(sDistrictNum);
            executeQuery("N_CREATE_ORDER_LINE",
                    orderNumber, customerDistrictId, customerWarehouseId, i, itemNumber, supplierWarehouse, quantity, itemAmount, sDistInfo, itemName);

            //2.3 Update item's customer list
            String customerFullId = customerWarehouseId + " " + customerDistrictId + " " + customerId;
            executeQuery("N_UPDATE_ITEM_CUSTOMER_LIST", customerFullId, itemNumber);
        }

        //3. compute total amount
        BigDecimal districtTax = districtInfo.getDecimal(CqlQueries.N_D_TAX_INDEX);
        BigDecimal warehouseTax = districtInfo.getDecimal(CqlQueries.N_W_TAX_INDEX);
        Row customerInfo = executeQuery("N_GET_CUSTOMER_INFO", customerWarehouseId, customerDistrictId, customerId).get(0);
        BigDecimal customerDiscount = customerInfo.getDecimal(CqlQueries.N_C_DISCOUNT_INDEX);
        BigDecimal totalTaxes = BigDecimal.ONE.add(districtTax.add(warehouseTax));
        totalAmount = totalAmount.multiply(totalTaxes.multiply(BigDecimal.ONE.subtract(customerDiscount)));
        String customerFirstName = customerInfo.getString(CqlQueries.N_C_FIRST_INDEX);
        String customerMiddleName = customerInfo.getString(CqlQueries.N_C_MIDDLE_INDEX);
        String customerLastName = customerInfo.getString(CqlQueries.N_C_LAST_INDEX);

        //4. Create Order
        // add C_FIRST, C_MIDDLE_C_LAST from customer too
        // brought to the end after creating all order lines to avoid an extra iteration for checking if warehouses are local
        // TODO: check if string to represent timestamp for O_ENTRY works
        String entryDate = TimeHelper.formatDate(new Date());
        executeQuery("N_CREATE_ORDER", orderNumber, customerDistrictId, customerWarehouseId, customerId, entryDate, numOfItems, isAllLocal, customerFirstName, customerMiddleName, customerLastName);
        executeQuery("N_CREATE_ORDER_SMALL", orderNumber, customerDistrictId, customerWarehouseId, customerId, entryDate, customerFirstName, customerMiddleName, customerLastName);

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

        System.out.println("Finish executing New Order Transaction...");
    }
}
