import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;

// Problems: cannot specify type length
public class CreateTable {
    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1")
                //.withCredentials("jeff", "i6XJsj!k#9")
                .build();

        // create session on the "wholesale" keyspace
        Session session = cluster.connect("wholesale");

        // Drop table if exists
        session.execute(SchemaBuilder.dropTable("Warehouse").ifExists());
        session.execute(SchemaBuilder.dropTable("District").ifExists());
        session.execute(SchemaBuilder.dropTable("Customer").ifExists());
        session.execute(SchemaBuilder.dropTable("Order_New").ifExists()); // Order is a reserved keyword
        session.execute(SchemaBuilder.dropTable("Item").ifExists());
        session.execute(SchemaBuilder.dropTable("Order_Line").ifExists());
        session.execute(SchemaBuilder.dropTable("Stock").ifExists());

        // Warehouse
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
        SchemaStatement customerSchemaStatement = SchemaBuilder.createTable("Customer").
                addPartitionKey("C_W_ID", DataType.cint()). //pk
                addPartitionKey("C_D_ID", DataType.cint()). //pk
                addPartitionKey("C_ID", DataType.cint()). //pk
                addPartitionKey("C_FIRST", DataType.varchar()).
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
                addColumn("C_BALANCE", DataType.decimal()). //ck cannot do clustering keys on decimal, performance worse anyway
                addColumn("C_YTD_PAYMENT", DataType.cfloat()).
                addColumn("C_PAYMENT_CNT", DataType.cint()).
                addColumn("C_DELIVERY_CNT", DataType.cint()).
                addColumn("C_DATA", DataType.varchar());
        session.execute(customerSchemaStatement);

        // Order
        SchemaStatement orderSchemaStatement = SchemaBuilder.createTable("Order_New").
                addPartitionKey("O_W_ID", DataType.cint()). //pk
                addPartitionKey("O_D_ID", DataType.cint()). //pk
                addPartitionKey("O_ID", DataType.cint()). //pk
                addColumn("O_C_ID", DataType.cint()).
                addColumn("O_CARRIER_ID", DataType.cint()).
                addColumn("O_OL_CNT", DataType.decimal()).
                addColumn("O_ALL_LOCAL", DataType.decimal()).
                addColumn("O_ENTRY", DataType.timestamp()).
                addColumn("C_FIRST", DataType.varchar()). //from customer
                addColumn("C_MIDDLE", DataType.varchar()). //from customer
                addColumn("C_LAST", DataType.varchar()); //from customer
        session.execute(orderSchemaStatement);

        // Item
        SchemaStatement itemSchemaStatement = SchemaBuilder.createTable("Item").
                addPartitionKey("I_ID", DataType.cint()). //pk
                addColumn("I_NAME", DataType.varchar()).
                addColumn("I_PRICE", DataType.decimal()).
                addColumn("I_IM_ID", DataType.cint()).
                addColumn("I_DATA", DataType.varchar()).
                addColumn("I_O_ID_LIST", DataType.set(DataType.varchar())); //from self
        session.execute(itemSchemaStatement);

        // Order-Line
        SchemaStatement orderlineSchemaStatement = SchemaBuilder.createTable("Order_Line").
                addPartitionKey("OL_W_ID", DataType.cint()). //pk
                addPartitionKey("OL_D_ID", DataType.cint()). //pk
                addPartitionKey("OL_O_ID", DataType.cint()). //pk
                addPartitionKey("OL_NUMBER", DataType.cint()). //pk
                addColumn("OL_I_ID", DataType.cint()).
                addColumn("OL_DELIVERY_D", DataType.timestamp()).
                addColumn("OL_AMOUNT", DataType.decimal()).
                addColumn("OL_SUPPLY_W_ID", DataType.cint()).
                addColumn("OL_QUANTITY", DataType.decimal()).
                addColumn("OL_DIST_INFO", DataType.varchar()).
                addColumn("I_NAME", DataType.varchar()); //from item
        session.execute(orderlineSchemaStatement);

        // Stock
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

        Metadata metadata = cluster.getMetadata();
        System.out.println("Schema:");
        System.out.println(metadata.exportSchemaAsString());
        System.out.println();

        System.out.printf("Schema agreement : %s\n",
                metadata.checkSchemaAgreement());

        // close and exit
        cluster.close();
        System.exit(0);
    }
}
