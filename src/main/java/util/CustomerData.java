package util;

import java.math.BigDecimal;

public class CustomerData implements Comparable<CustomerData>{
    public int warehouseId;
    public int districtId;
    public int customerId;
    public BigDecimal balance;
    public String firstName;
    public String middleName;
    public String lastName;
    public String warehouseName;
    public String districtName;


    public CustomerData(int warehouseId, int districtId, int customerId, BigDecimal balance, String firstName, String middleName, String lastName, String warehouseName, String districtName) {
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.customerId = customerId;
        this.balance = balance;
        this.firstName = firstName;
        this.middleName = middleName;
        this.lastName = lastName;
        this.warehouseName = warehouseName;
        this.districtName = districtName;
    }

    @Override
    public int compareTo(CustomerData obj) {
        if(obj == this) {
            return 0;
        }
        return balance.compareTo(obj.balance);
    }
}
