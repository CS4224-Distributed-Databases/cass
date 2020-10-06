package util;

import java.math.BigDecimal;

public class ItemData {
    public int itemId;
    public String itemName;
    public BigDecimal quantity;

    public ItemData(int itemId, String itemName, BigDecimal quantity) {
        this.itemId = itemId;
        this.itemName = itemName;
        this.quantity = quantity;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ItemData)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        return this.itemId == ((ItemData) obj).itemId && this.itemName == ((ItemData) obj).itemName && this.quantity.equals(((ItemData) obj).quantity);
    }

    @Override
    public int hashCode() {
        return itemId;
    }
}
