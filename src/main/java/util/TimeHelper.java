package util;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Date;

public class TimeHelper {

    private static DateTimeFormatter formatter  = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendLiteral('-')
            .appendPattern("MM")
            .appendLiteral('-')
            .appendPattern("dd")
            .appendLiteral(' ')
            .appendPattern("HH")
            .appendLiteral(':')
            .appendPattern("mm")
            .appendLiteral(':')
            .appendPattern("ss")
            .appendPattern(".")
            .optionalStart().appendPattern("SSS").optionalEnd() // Some time stamps might have 1, 2 or 3 SS
            .optionalStart().appendPattern("SS").optionalEnd() // Hence make it optional
            .optionalStart().appendPattern("S").optionalEnd()
            .toFormatter()
            .withZone(ZoneId.of("UTC")); //timestamp TODO must remove timezone

    public static String formatDate(Date date) {
        return formatter.format(date.toInstant());
    }

    public static DateTimeFormatter getFormatter(){
        return formatter;
    }
}
