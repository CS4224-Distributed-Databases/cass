import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
public class Insert {
    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1")
                //.withCredentials("jeff", "i6XJsj!k#9")
                .build();

        // create session on the "hotel" keyspace
        Session session = cluster.connect("hotel");

        // create parameterized INSERT statement
        SimpleStatement hotelInsert = new SimpleStatement(
                "INSERT INTO hotels (id, name, phone) VALUES (?, ?, ?) IF NOT EXISTS",
                "AZ333", "Super Hotel at WestWorld", "1-888-999-9999");

        ResultSet hotelInsertResult = session.execute(hotelInsert);

        // print results
        System.out.println("Inserted: ");
        System.out.println(hotelInsertResult.wasApplied());

        cluster.close();
        System.exit(0);
    }
}
