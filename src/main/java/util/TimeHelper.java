package util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeHelper {

    //TODO: check if this is the correct format expected
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String formatDate(Date date) {
        return formatter.format(date);
    }

}
