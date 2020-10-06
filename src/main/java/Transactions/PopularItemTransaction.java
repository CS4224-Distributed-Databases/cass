package Transactions;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import util.CqlQueries;
import util.ItemData;
import util.TimeHelper;

import java.math.BigDecimal;
import java.util.*;

public class PopularItemTransaction extends BaseTransaction {

    private int warehouseId;
    private int districtId;
    private int numOfLastOrders;

    public PopularItemTransaction(Session session, HashMap<String, PreparedStatement> insertPrepared) {
        super(session, insertPrepared);
    }

    @Override
    public void parseInput(Scanner sc, String inputLine) {
        // PopularItem expects format of I,W_ID,D_ID,L.
        String[] input = inputLine.split(",");
        assert(input[0].equals("I"));
        this.warehouseId = Integer.parseInt(input[1]);
        this.districtId = Integer.parseInt(input[2]);
        this.numOfLastOrders = Integer.parseInt(input[3]);
    }

    @Override
    public void execute() {

        System.out.println("Starting Execution of Popular Item Transaction...");
        prepareStatement("I_GET_DISTRICT_NEXT_ORDER_NUM", CqlQueries.I_GET_DISTRICT_NEXT_ORDER_NUM);
        prepareStatement("I_GET_ORDER_INFO", CqlQueries.I_GET_ORDER_INFO);
        prepareStatement("I_GET_ORDER_LINES", CqlQueries.I_GET_ORDER_LINES);

        // 1. get N = next available order no
        int nextOrderNum  = executeQuery("I_GET_DISTRICT_NEXT_ORDER_NUM", warehouseId, districtId).get(0).getInt(CqlQueries.I_D_NEXT_O_ID_INDEX);

        // 2. iterate through orderNum = [N-L to N)
        // lookup the orderlines with this orderNum and find the most popular items among all these orderlines
        // TODO: Did not use the range query for orderNum <- not sure if it would be slower/faster
        //  as it would have overhead of clustering (order entries are created with every newOrder xact and take up 40%),
        //  would need to group by orderId later to get the most popular item
        //  Can potentially remove orderid as clustering key and use it as primary key instead?

        HashMap<Integer, Integer> numOfOrdersPerItem = new HashMap<>(); // <itemId, numOfOrdersThisItemAppearsIn>
        HashMap<Integer, List<ItemData>> popularItemsPerOrder = new HashMap<>();
        HashMap<Integer, Row> orderInfoMap = new HashMap<>(); // <orderNum, orderRow>

        for (int orderNum = nextOrderNum - numOfLastOrders; orderNum < nextOrderNum; orderNum++) {
            HashSet<Integer> seenItemsInThisOrder = new HashSet<>();
            List<Row> orderInfos = executeQuery("I_GET_ORDER_INFO", warehouseId, districtId, orderNum);
            if (orderInfos.size() == 0) {
                // no such order with this order number
                continue;
            }
            orderInfoMap.put(orderNum, orderInfos.get(0));

            List<Row> orderLinesInfos = executeQuery("I_GET_ORDER_LINES", warehouseId, districtId, orderNum);
            if (orderLinesInfos.size() == 0) {
                // no orderLines for this order
                popularItemsPerOrder.put(orderNum, null);
                continue;
            }

            BigDecimal maxQuantity = BigDecimal.ZERO;
            List<ItemData> candidatePopularItems = new ArrayList<>();
            for (Row orderLineInfo: orderLinesInfos) {
                BigDecimal olQuantity = orderLineInfo.getDecimal(CqlQueries.I_OL_QUANTITY_INDEX);
                int itemId = orderLineInfo.getInt(CqlQueries.I_OL_I_ID_INDEX);

                if (olQuantity.compareTo(maxQuantity) > 0) {
                    maxQuantity = olQuantity;
                    candidatePopularItems = new ArrayList<>();
                    candidatePopularItems.add(
                            new ItemData(itemId, orderLineInfo.getString(CqlQueries.I_OL_I_NAME_INDEX), olQuantity)
                    );
                } else if (olQuantity.compareTo(maxQuantity) == 0) {
                    candidatePopularItems.add(
                            new ItemData(itemId, orderLineInfo.getString(CqlQueries.I_OL_I_NAME_INDEX), olQuantity)
                    );
                }
                if(!seenItemsInThisOrder.contains(itemId)) {
                    seenItemsInThisOrder.add(itemId);
                    int numOfOrders = numOfOrdersPerItem.getOrDefault(itemId, 0);
                    numOfOrdersPerItem.put(itemId, numOfOrders+1);
                }
            }
            System.out.println("candidatePopularItems.size() "+ candidatePopularItems.size());
            popularItemsPerOrder.put(orderNum, candidatePopularItems);
        }

        // 3. Print output
        System.out.println(String.format("1. District: (%d, %d)", warehouseId, districtId));
        System.out.println(String.format("2. Number of last orders: %d", numOfLastOrders));
        System.out.println("3. Iterating through orders");

        HashSet<ItemData> distinctPopularItems = new HashSet<>(); // for calculating percentages later

        for (int orderNum = nextOrderNum - numOfLastOrders; orderNum < nextOrderNum; orderNum++) {
            Row orderInfo = orderInfoMap.get(orderNum);
            String entryDate = TimeHelper.formatDate(orderInfo.getTimestamp(CqlQueries.I_O_ENTRY_INDEX));
            System.out.println(String.format("\t3.1 Order number: %d, Entry date: %s", orderNum, entryDate));
            System.out.println(String.format("\t3.2 Customer: (%s, %s, %s)", orderInfo.getString(CqlQueries.I_O_C_FIRST_INDEX), orderInfo.getString(CqlQueries.I_O_C_MIDDLE_INDEX), orderInfo.getString(CqlQueries.I_O_C_LAST_INDEX)));
            System.out.println("\t3.3 Popular Items: ");
            List<ItemData> popularItemsInThisOrder = popularItemsPerOrder.get(orderNum);
            if (popularItemsInThisOrder == null) {
                System.out.println("\t\t none");
                continue;
            }
            for(ItemData itemData: popularItemsInThisOrder) {
                System.out.println(String.format("\t\t Item name: %s", itemData.itemName));
                System.out.println(String.format("\t\t Quantity: %.0f", itemData.quantity));
                distinctPopularItems.add(itemData);
            }
        }

        //TODO: check this part if percentage is calculated correctly
        System.out.println("4. For each distinct popular item");
        for (ItemData itemData: distinctPopularItems) {
            int numOfOrders = numOfOrdersPerItem.get(itemData.itemId);
            double percentage =  ((double) numOfOrders / numOfLastOrders) * 100.0;
            System.out.println(String.format("\tItem name: %s", itemData.itemName));
            System.out.println(String.format("\tPercentage of last orders containing this: %.2f%%", percentage));
        }

        System.out.println("Finish executing Popular Item Transactions...");
    }

}
