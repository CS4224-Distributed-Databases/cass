package DataLoader;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;

public class CreateTables {

    public CreateTables(Session session) {
        // Drop table if exists
        try {
            System.out.println("Drop tables");
            session.execute("DROP MATERIALIZED VIEW IF EXISTS CS4224.Customer_Balance");
            session.execute(SchemaBuilder.dropTable("Warehouse").ifExists());
            session.execute(SchemaBuilder.dropTable("District").ifExists());
            session.execute(SchemaBuilder.dropTable("Customer").ifExists());
            session.execute(SchemaBuilder.dropTable("Order_New").ifExists()); // Order is a reserved keyword
            session.execute(SchemaBuilder.dropTable("Order_Small").ifExists());
            session.execute(SchemaBuilder.dropTable("Item").ifExists());
            session.execute(SchemaBuilder.dropTable("Order_Line").ifExists());
            session.execute(SchemaBuilder.dropTable("Stock").ifExists());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Warehouse
        System.out.println("Create Warehouse Table");
        SchemaStatement warehouseSchemaStatement = SchemaBuilder.createTable("Warehouse").
                addPartitionKey("W_ID", DataType.cint()). //pk
                addColumn("W_NAME", DataType.varchar()).
                addColumn("W_STREET_1", DataType.varchar()).
                addColumn("W_STREET_2", DataType.varchar()).
                addColumn("W_CITY", DataType.varchar()).
                addColumn("W_STATE", DataType.varchar()).
                addColumn("W_ZIP", DataType.varchar()).
                addColumn("W_TAX", DataType.decimal()).
                addColumn("W_YTD", DataType.decimal());
        session.execute(warehouseSchemaStatement);

        // District
        System.out.println("Create District Table");
        SchemaStatement districtSchemaStatement = SchemaBuilder.createTable("District").
                addPartitionKey("D_W_ID", DataType.cint()). //pk
                addPartitionKey("D_ID", DataType.cint()). //pk
                addColumn("D_NAME", DataType.varchar()).
                addColumn("D_STREET_1", DataType.varchar()).
                addColumn("D_STREET_2", DataType.varchar()).
                addColumn("D_CITY", DataType.varchar()).
                addColumn("D_STATE", DataType.varchar()).
                addColumn("D_ZIP", DataType.varchar()).
                addColumn("D_TAX", DataType.decimal()).
                addColumn("D_YTD", DataType.decimal()).
                addColumn("D_NEXT_O_ID", DataType.cint()).
                addColumn("W_TAX", DataType.decimal()); //from warehouse
        session.execute(districtSchemaStatement);

        // Customer
        System.out.println("Create Customer Table");
        SchemaStatement customerSchemaStatement = SchemaBuilder.createTable("Customer").
                addPartitionKey("C_W_ID", DataType.cint()). //pk
                addPartitionKey("C_D_ID", DataType.cint()). //pk
                addPartitionKey("C_ID", DataType.cint()). //pk
                addColumn("C_FIRST", DataType.varchar()).
                addColumn("C_MIDDLE", DataType.varchar()).
                addColumn("C_LAST", DataType.varchar()).
                addColumn("C_STREET_1", DataType.varchar()).
                addColumn("C_STREET_2", DataType.varchar()).
                addColumn("C_CITY", DataType.varchar()).
                addColumn("C_STATE", DataType.varchar()).
                addColumn("C_ZIP", DataType.varchar()).
                addColumn("C_PHONE", DataType.varchar()).
                addColumn("C_SINCE", DataType.timestamp()).
                addColumn("C_CREDIT", DataType.varchar()).
                addColumn("C_CREDIT_LIM", DataType.decimal()).
                addColumn("C_DISCOUNT", DataType.decimal()).
                addColumn("C_BALANCE", DataType.decimal()).
                addColumn("C_YTD_PAYMENT", DataType.cfloat()).
                addColumn("C_PAYMENT_CNT", DataType.cint()).
                addColumn("C_DELIVERY_CNT", DataType.cint()).
                addColumn("C_DATA", DataType.varchar()).
                addColumn("C_D_NAME", DataType.varchar()).
                addColumn("C_W_NAME", DataType.varchar());
        session.execute(customerSchemaStatement);

        // Order
        System.out.println("Create Order_New Table");
        SchemaStatement orderSchemaStatement = SchemaBuilder.createTable("Order_New").
                addClusteringColumn("O_C_ID", DataType.cint()).
                addClusteringColumn("O_ID", DataType.cint()). // lower level clustering key
                addPartitionKey("O_W_ID", DataType.cint()). //pk
                addPartitionKey("O_D_ID", DataType.cint()). //pk
                addColumn("O_CARRIER_ID", DataType.cint()).
                addColumn("O_OL_CNT", DataType.decimal()).
                addColumn("O_ALL_LOCAL", DataType.decimal()).
                addColumn("O_ENTRY", DataType.timestamp()).
                addColumn("O_C_FIRST", DataType.varchar()). //from customer
                addColumn("O_C_MIDDLE", DataType.varchar()). //from customer
                addColumn("O_C_LAST", DataType.varchar()); //from customer
        session.execute(orderSchemaStatement);


        // Order_Small - to have O_ID as the higher level clustering key for Popular Item transaction
        // which does not have knowledge of the customerId
        //TODO: Rmb to update all load data and xacts to update this smaller table too
        System.out.println("Create Order_Small Table");
        SchemaStatement orderSmallSchemaStatement = SchemaBuilder.createTable("Order_Small").
                addClusteringColumn("O_ID", DataType.cint()).
                addClusteringColumn("O_C_ID", DataType.cint()). // lower level clustering key
                addPartitionKey("O_W_ID", DataType.cint()). //pk
                addPartitionKey("O_D_ID", DataType.cint()). //pk
                addColumn("O_CARRIER_ID", DataType.cint()).
                addColumn("O_ENTRY", DataType.timestamp()).
                addColumn("O_C_FIRST", DataType.varchar()). //from customer
                addColumn("O_C_MIDDLE", DataType.varchar()). //from customer
                addColumn("O_C_LAST", DataType.varchar()); //from customer
        session.execute(orderSmallSchemaStatement);

        // Item
        System.out.println("Create Item Table");
        SchemaStatement itemSchemaStatement = SchemaBuilder.createTable("Item").
                addPartitionKey("I_ID", DataType.cint()). //pk
                addColumn("I_NAME", DataType.varchar()).
                addColumn("I_PRICE", DataType.decimal()).
                addColumn("I_IM_ID", DataType.cint()).
                addColumn("I_DATA", DataType.varchar()).
                addColumn("I_O_ID_LIST", DataType.list(DataType.varchar())); //from self
        session.execute(itemSchemaStatement);

        // Order-Line
        System.out.println("Create Order_Line Table");
        SchemaStatement orderlineSchemaStatement = SchemaBuilder.createTable("Order_Line").
                addClusteringColumn("OL_NUMBER", DataType.cint()).
                addPartitionKey("OL_W_ID", DataType.cint()). //pk
                addPartitionKey("OL_D_ID", DataType.cint()). //pk
                addPartitionKey("OL_O_ID", DataType.cint()). //pk
                addColumn("OL_I_ID", DataType.cint()).
                addColumn("OL_DELIVERY_D", DataType.timestamp()).
                addColumn("OL_AMOUNT", DataType.decimal()).
                addColumn("OL_SUPPLY_W_ID", DataType.cint()).
                addColumn("OL_QUANTITY", DataType.decimal()).
                addColumn("OL_DIST_INFO", DataType.varchar()).
                addColumn("OL_I_NAME", DataType.varchar()); //from item
        session.execute(orderlineSchemaStatement);

        // Stock
        System.out.println("Create Stock Table");
        SchemaStatement stockSchemaStatement = SchemaBuilder.createTable("Stock").
                addPartitionKey("S_W_ID", DataType.cint()). //pk
                addPartitionKey("S_I_ID", DataType.cint()). //pk
                addColumn("S_QUANTITY", DataType.decimal()).
                addColumn("S_YTD", DataType.decimal()).
                addColumn("S_ORDER_CNT", DataType.cint()).
                addColumn("S_REMOTE_CNT", DataType.cint()).
                addColumn("S_DIST_01", DataType.varchar()).
                addColumn("S_DIST_02", DataType.varchar()).
                addColumn("S_DIST_03", DataType.varchar()).
                addColumn("S_DIST_04", DataType.varchar()).
                addColumn("S_DIST_05", DataType.varchar()).
                addColumn("S_DIST_06", DataType.varchar()).
                addColumn("S_DIST_07", DataType.varchar()).
                addColumn("S_DIST_08", DataType.varchar()).
                addColumn("S_DIST_09", DataType.varchar()).
                addColumn("S_DIST_10", DataType.varchar()).
                addColumn("S_DATA", DataType.varchar());
        session.execute(stockSchemaStatement);

        // Create materialised view for Customer Balance

        System.out.println("Create materialised view for Customer_Balance");
        String createCustomerBalanceView = "CREATE MATERIALIZED VIEW CS4224.Customer_Balance " +
                "AS SELECT C_W_ID, C_D_ID, C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST, C_W_NAME, C_D_NAME FROM CS4224.Customer " +
                "WHERE C_W_ID IS NOT NULL and C_D_ID IS NOT NULL and C_ID IS NOT NULL and C_BALANCE IS NOT NULL and C_FIRST IS NOT NULL and C_MIDDLE IS NOT NULL and C_LAST IS NOT NULL and C_W_NAME IS NOT NULL and C_D_NAME IS NOT NULL " +
                "PRIMARY KEY ((C_W_ID, C_D_ID), C_BALANCE, C_ID)";
        session.execute(createCustomerBalanceView);
    }
}