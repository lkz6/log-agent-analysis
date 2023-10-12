package com.hzx.sink;

import com.hzx.util.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public class ElasticSearchUtils {

    public static String getIndexName(String time, String datePattern) {
        if (StringUtils.isBlank(time)) {
            return DateUtils.getNowDay();
        }
        DateTime dateTime = DateTime.parse(time, DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
        return dateTime.toLocalDateTime().toString(datePattern);
    }

}
