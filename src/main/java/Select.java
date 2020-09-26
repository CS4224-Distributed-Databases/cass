import com.datastax.driver.core.*;

public class Select {
    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1")
                //.withCredentials("jeff", "i6XJsj!k#9")
                .build();

        // create session on the "hotel" keyspace
        Session session = cluster.connect("hotel");

        // create parameterized SELECT statement
        SimpleStatement hotelSelect = new SimpleStatement(
                "SELECT id, name FROM hotels WHERE id=?",
                "B299");

        ResultSet hotelSelectResult = session.execute(hotelSelect);
        Row hotelInfo = hotelSelectResult.all().get(0);
        // print results
        System.out.println("Selected: ");
        System.out.println(hotelInfo.getString(0));

        cluster.close();
        System.exit(0);
    }
}
