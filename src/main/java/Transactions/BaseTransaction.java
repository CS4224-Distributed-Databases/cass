package Transactions;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import java.util.List;

public abstract class BaseTransaction {
    Session session;

    public BaseTransaction(Session session) {
        this.session = session;
    }

    public abstract void parseInput(String[] input);

    public abstract void execute();

    public List<Row> executeCqlQuery(String query, List<Object> args) {
        SimpleStatement queryStatement = new SimpleStatement(query, args);
        ResultSet queryResult = session.execute(queryStatement);
        List<Row> resultSet = queryResult.all();
        return resultSet;
    }
}
