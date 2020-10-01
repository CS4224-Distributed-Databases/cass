package Transactions;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public abstract class BaseTransaction {
    Session session;
    HashMap<String, PreparedStatement> insertPrepared;

    public BaseTransaction(Session session, HashMap<String, PreparedStatement> insertPrepared) {
        this.session = session;
        this.insertPrepared = insertPrepared;
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
        BoundStatement insertBound = this.insertPrepared.get(queryKey).bind(args);
        ResultSet queryResult = session.execute(insertBound);
        List<Row> resultSet = queryResult.all();
        return resultSet;
    }
}
