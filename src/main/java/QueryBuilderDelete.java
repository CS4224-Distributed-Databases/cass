import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

// FILE NOT IN USE
public class QueryBuilderDelete {
    public static void main(String[] args) {

        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1")
                //.withCredentials("jeff", "i6XJsj!k#9").
                .build();

        // create session on the "hotel" keyspace
        Session session = cluster.connect("hotel");

        // create a Hotel ID
        String id="AZ123";

        // build an INSERT statement
        BuiltStatement hotelInsertBuilt = QueryBuilder.insertInto("hotels").
                value("id", id).
                value("name", "Super Hotel at WestWorld").
                value("phone", "1-888-999-9999");
        ResultSet hotelInsertResult = session.execute(hotelInsertBuilt);

        System.out.println(hotelInsertResult);
        System.out.println("Inserted: " + hotelInsertResult.wasApplied());

        // build a SELECT statement
        BuiltStatement hotelSelectBuilt = QueryBuilder.select().all().
                from("hotels").where(eq("id", id));
        ResultSet hotelSelectResult = session.execute(hotelSelectBuilt);
        System.out.println("SELECT statement: " + hotelSelectResult.wasApplied());
        // System.out.println(hotelSelectResult.all()); can also use this, will exhaust the result

        // print results
        System.out.println("Selected row exists:");
        // Sometimes the id field got error, remove, run, and put back in, then works its weird af
        for (Row row : hotelSelectResult) {
            System.out.format("id: %s, name: %s, phone: %s\n", row.getString("id"),
                    row.getString("name"), row.getString("phone"));
        }

        // build a DELETE statement
        BuiltStatement hotelDeleteBuilt = QueryBuilder.delete().all().
                from("hotels").where(eq("id", id));
        ResultSet hotelDeleteResult = session.execute(hotelDeleteBuilt);

        // result metadata
        System.out.println("Deleted: " + hotelDeleteResult.wasApplied());

        // close and exit
        cluster.close();
        System.exit(0);
    }
}
