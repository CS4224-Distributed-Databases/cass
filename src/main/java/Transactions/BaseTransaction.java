package Transactions;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import java.util.HashMap;
import java.util.List;

public abstract class BaseTransaction {
    Session session;
    HashMap<String, PreparedStatement> insertPrepared;

    public BaseTransaction(Session session) {
        this.session = session;
        this.insertPrepared = new HashMap<>();
    }

    public abstract void parseInput(String[] input);

    public abstract void execute();

    // TODO: TO rewrite other transactions that uses this function to use the below two function instead
    // TODO: and then DELETE THIS function:
    public List<Row> executeCqlQuery(String query, List<Object> args) {
        SimpleStatement queryStatement = new SimpleStatement(query, args);
        ResultSet queryResult = session.execute(queryStatement);
        List<Row> resultSet = queryResult.all();
        return resultSet;
    }

    public void prepareStatement(String queryKey, String query){
        if (this.insertPrepared.containsKey(queryKey)){
            return;
        }
        this.insertPrepared.put(queryKey, session.prepare(query));
    }

    public List<Row> executeQuery(String queryKey, Object... args){
        BoundStatement insertBound = this.insertPrepared.get(queryKey).bind(args);
        ResultSet queryResult = session.execute(insertBound);
        List<Row> resultSet = queryResult.all();
        return resultSet;
    }
}
