package Transactions;

import com.datastax.driver.core.*;

import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public abstract class BaseTransaction {
    Session session;
    HashMap<String, PreparedStatement> insertPrepared;
    String consistencyType;

    public BaseTransaction(Session session, HashMap<String, PreparedStatement> insertPrepared, String consistencyType) {
        this.session = session;
        this.insertPrepared = insertPrepared;
        this.consistencyType = consistencyType;
    }

    public abstract void parseInput(Scanner sc, String inputLine);

    public abstract void execute();

    public void prepareStatement(String queryKey, String query){
        if (this.insertPrepared.containsKey(queryKey)){
            return;
        }
        this.insertPrepared.put(queryKey, session.prepare(query));
    }

    public List<Row> executeQuery(String queryKey, Object... args){
        BoundStatement insertBound = this.insertPrepared.get(queryKey).setConsistencyLevel(getConsistencyLevel(queryKey)).bind(args);
        ResultSet queryResult = session.execute(insertBound);
        List<Row> resultSet = queryResult.all();
        return resultSet;
    }

    private ConsistencyLevel getConsistencyLevel(String query) {
        if (this.consistencyType.equals("QUORUM")) {
            return ConsistencyLevel.QUORUM;
        } else {
            if (query.contains("SELECT")) {
                //read
                return ConsistencyLevel.ONE;
            } else {
                //write
                return ConsistencyLevel.ALL;
            }
        }
    }
}
