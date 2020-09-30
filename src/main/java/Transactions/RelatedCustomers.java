package Transactions;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import util.CqlQueries;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import static util.TimeHelper.formatDate;

public class RelatedCustomers extends BaseTransaction {
    private static final int NUM_DISTRICTS = 10;
    private int warehouseID;
    private int districtID;
    private int customerID;

    public RelatedCustomers(Session session) {
        super(session);
    }

    @Override
    public void parseInput(String[] input) {
        // Related Customers expects format of R, W_ID, D_ID, C_ID
        assert(input[0].equals("R"));
        this.warehouseID = Integer.parseInt(input[1]);
        this.districtID = Integer.parseInt(input[2]);
        this.customerID = Integer.parseInt(input[3]);
    }


    @Override
    public void execute() {

        System.out.println("Starting Execution of Related Customers Transaction...");
        prepareStatement("YET_DELIVERED_ORDER", CqlQueries.YET_DELIVERED_ORDER);


        System.out.println("Finish executing Delivery Transactions...");
    }
}
