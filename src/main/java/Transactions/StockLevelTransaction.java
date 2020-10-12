package Transactions;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import util.CqlQueries;

import java.math.BigDecimal;
import java.util.*;

public class StockLevelTransaction extends BaseTransaction {
    private int warehouseId;
    private int districtId;
    private BigDecimal stockThreshold;
    private int ordersToExamine;

    public StockLevelTransaction(Session session, HashMap<String, PreparedStatement> insertPrepared,  String consistencyType) {
        super(session, insertPrepared, consistencyType);
    }

    @Override
    public void parseInput(Scanner sc, String inputLine) {
        // Payment expects format of S,W ID,D ID, T, L.
        String[] input = inputLine.split(",");
        assert(input[0].equals("S"));
        warehouseId = Integer.parseInt(input[1]);
        districtId = Integer.parseInt(input[2]);
        stockThreshold = new BigDecimal((input[3]));
        ordersToExamine = Integer.parseInt(input[4]);
    }

    @Override
    public void execute() {

        System.out.println("Starting Execution of Stock Level Transaction...");
        prepareStatement("S_GET_DISTRICT", CqlQueries.S_GET_DISTRICT);
        prepareStatement("S_GET_LAST_L_ORDERS", CqlQueries.S_GET_LAST_L_ORDERS);
        prepareStatement("S_GET_STOCK_ITEMS", CqlQueries.S_GET_STOCK_ITEMS);

        Row districtInfo = executeQuery("S_GET_DISTRICT", warehouseId, districtId).get(0);
        Integer nextOID = districtInfo.getInt(CqlQueries.STOCK_D_NEXT_OID_INDEX);
        // current nextOID on this test data is 3001, but data in orderline table not complete
        // hardcode to 10 instead for testing
        // nextOID = 10;
        Integer startingFromOrder = nextOID - ordersToExamine;
        List<Row> orders = executeQuery("S_GET_LAST_L_ORDERS", warehouseId, districtId, startingFromOrder, nextOID);

        Set<Integer> itemIDs = new HashSet<>();
        for (Row order : orders) {
            Integer itemId = order.getInt(CqlQueries.STOCK_OL_OID_INDEX);
            itemIDs.add(itemId);
        }

        // current set on this test data is large like {34223,756456,3455} which return no stock since data in stock table not complete
        // hardcode to {1,2} instead for testing
//        itemIDs.clear();
//        itemIDs.add(1);
//        itemIDs.add(2);

        Integer itemsBelowThresholdCount = 0;
        for (Integer itemID : itemIDs) {
            Row itemStock = executeQuery("S_GET_STOCK_ITEMS", warehouseId, itemID).get(0);
            BigDecimal itemStockQuantity= itemStock.getDecimal(CqlQueries.STOCK_QUANTITY_INDEX);
            if (itemStockQuantity.compareTo(stockThreshold) < 0) {
                itemsBelowThresholdCount++;
            }
        }

        System.out.printf("Number of items below threshold: %d\n", itemsBelowThresholdCount);
        System.out.println("Finish executing Stock Level Transaction...");
    }
}
//S,1,1,30,4
// one has 96 level, one is 26 level