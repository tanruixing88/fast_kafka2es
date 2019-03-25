package util;

import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;

/**
 * @author tanruixing  
 * Created on 2019-03-21
 */
public class Common {
    public static boolean isNumeric(String str) {
        for (int i = 0; i < str.length(); i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    public static String getFmtDtIndexNameByRegex(String indexFormat, DateTime dt, String regex) {
        Pattern patternFormat = Pattern.compile(regex);
        Matcher matcherFormat = patternFormat.matcher(indexFormat);
        if (matcherFormat.find()) {
            String subFormat = indexFormat.substring(matcherFormat.start(), matcherFormat.end());
            SimpleDateFormat format = new SimpleDateFormat(subFormat);
            String prefix = indexFormat.substring(0, matcherFormat.start());
            String suffix = indexFormat.substring(matcherFormat.end(), indexFormat.length());
            String curDtStr = format.format(dt.toDate());
            return new StringBuilder(prefix).append(curDtStr).append(suffix).toString();
        }

        return null;
    }

    public static String getIndexNameByFmtDt(String indexFormat, DateTime dt) {
        String regexFormatHour = "yyyy[\\.\\-_/]?MM[\\.\\-_/]?dd[\\.\\-_/]?HH";
        String indexNameHour = getFmtDtIndexNameByRegex(indexFormat, dt, regexFormatHour);
        if (indexNameHour != null) {
            return indexNameHour;
        }

        String regexFormatDay = "yyyy[\\.\\-_/]?MM[\\.\\-_/]?dd";
        String indexNameDay = getFmtDtIndexNameByRegex(indexFormat, dt, regexFormatDay);
        if (indexNameDay != null) {
            return indexNameDay;
        }

        return null;
    }
}
