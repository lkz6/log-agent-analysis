package com.hzx.util;

import com.hzx.common.DatePattern;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;


public class DateUtils {
    public static Date MAX_DATE = new Date(Long.MAX_VALUE);
    private final static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMddHHmmss.SSS");
    private final static DateTimeFormatter yearFormat = DateTimeFormat.forPattern("yyyy");
    private final static DateTimeFormatter fmt1 = DateTimeFormat.forPattern("yyyyMMdd");
    private final static SimpleDateFormat fmt2;
    static {
        fmt2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        fmt2.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
    }

    public static String getLocalTime(String utcTime) {
        DateTime dt = new DateTime(utcTime, DateTimeZone.getDefault());

        return dt.plusHours(8).toString();

    }

    public static String getLogTime(Long timeStamp){
        return fmt2.format(timeStamp);
    }
    
    public static String getLocalTime2(String utcTime) {
        LocalDateTime localDateTime = fmt.parseDateTime(utcTime).toLocalDateTime();

        return localDateTime.toString();

    }


    public static String getNowDay() {

        return fmt1.print(System.currentTimeMillis());


    }

    public static Long getMillis(){
        return  System.currentTimeMillis();
    }

    public static String getNowDay1() {

        return fmt2.format(System.currentTimeMillis());


    }
    public static String getYear() {

        return yearFormat.print(System.currentTimeMillis());


    }

    public static String convertTimeZones(String toTimeZoneString, String datePattern, String fromDateTime) {
        DateTimeZone toTimeZone = DateTimeZone.forID(toTimeZoneString);

        DateTimeFormatter outputFormatter
                = DateTimeFormat.forPattern(datePattern).withZone(toTimeZone);
        System.out.println(outputFormatter.parseDateTime(fromDateTime).toString());
        return "";
    }


    /**
     * String timeZoneConvert = timeZoneConvert(
     * new Date().getTime()
     * , "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
     * "Asia/Shanghai");
     * @return 如:2019-12-30T16:32:07.616+0800
     */
    public static String addHours(String predate, String prepattern) {
        if(StringUtils.isBlank(prepattern)){
            fmt2.format(System.currentTimeMillis());
        }
        SimpleDateFormat f = null;
        Date date =  null;
        try {
            if(prepattern.equals(DatePattern.TIMESTAMP_1.getPattern())  ){
                predate = getYear()+"-"+predate;
                prepattern = DatePattern.TIMESTAMP_4.getPattern();
            }
            f = new SimpleDateFormat(prepattern);
            Date d = f.parse(predate);
            date = org.apache.commons.lang3.time.DateUtils.addHours(d, 8);
        } catch (ParseException e) {
            e.printStackTrace();
            return  fmt2.format(System.currentTimeMillis());

        }
        return fmt2.format(date);
    }

    public static String timeZoneConvert(String predate, String prepattern) {
        if(StringUtils.isBlank(prepattern)){
            return fmt2.format(System.currentTimeMillis());
        }
        long longtime = 0L;
        SimpleDateFormat f = null;
        try {
            if(prepattern.equals(DatePattern.TIMESTAMP_1.getPattern())){
                predate = getYear()+"-"+predate;
                prepattern = DatePattern.TIMESTAMP_4.getPattern();
            }
            if (prepattern.equals(DatePattern.TIMESTAMP_18.getPattern())){
                predate = getYear()+predate.replaceAll("-","");
                prepattern = DatePattern.TIMESTAMP_19.getPattern();
            }
            if (prepattern.equals(DatePattern.TIMESTAMP_20.getPattern())){
                predate = getYear()+predate.replaceAll("-","");
                prepattern = DatePattern.TIMESTAMP_21.getPattern();
            }
            f = new SimpleDateFormat(prepattern);
            if (prepattern.equals("dd MMM yyyy HH:mm:ss.SSS")){
                f = new SimpleDateFormat("dd MMM yyyy HH:mm:ss.SSS",Locale.US);
            }
            Date d = f.parse(predate);
            longtime = d.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return fmt2.format(System.currentTimeMillis());
        }
        return fmt2.format(longtime);
    }
    //解析EEE MMM dd HH:mm:ss zzzz yyyy格式
    public static String timeZoneConvert_eee(String date) {
        try {
        SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzzz yyyy", Locale.US);
        format.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        Date fdate = format.parse(date);
        long time = fdate.getTime();
        return fmt2.format(time);
        } catch (ParseException e) {
            e.printStackTrace();
            return fmt2.format(System.currentTimeMillis());
        }
    }
    
    public static String timeZoneConvert_sys(String predate) {
        long longtime = 0L;
        SimpleDateFormat f = null;
        try {
            predate = predate + " " + DateUtils.getYear();
            f = new SimpleDateFormat("MMM dd HH:mm:ss yyyy",Locale.UK);
            Date d = f.parse(predate);
            longtime = d.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return fmt2.format(System.currentTimeMillis());
        }
        return fmt2.format(longtime);
    }


    public static String getIndexName(String time, String datePattern) {
        DateTime dateTime = DateTime.parse(time, DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")).minusHours(8);
        return dateTime.toLocalDateTime().toString(datePattern);
    }
    
    public static boolean isSameDay(Date date1, Date date2) {
        if (date1 == null || date2 == null) {
            throw new IllegalArgumentException("The dates must not be null");
        }
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(date1);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(date2);
        return isSameDay(cal1, cal2);
    }

    /**
     * <p>
     * Checks if two calendars represent the same day ignoring time.
     * </p>
     *
     * @param cal1 the first calendar, not altered, not null
     * @param cal2 the second calendar, not altered, not null
     * @return true if they represent the same day
     * @throws IllegalArgumentException if either calendar is <code>null</code>
     */
    public static boolean isSameDay(Calendar cal1, Calendar cal2) {
        if (cal1 == null || cal2 == null) {
            throw new IllegalArgumentException("The dates must not be null");
        }
        return (cal1.get(Calendar.ERA) == cal2.get(Calendar.ERA) &&
                cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR) && cal1.get(Calendar.DAY_OF_YEAR) == cal2.get(Calendar.DAY_OF_YEAR));
    }

    /**
     * <p>
     * Checks if a date is today.
     * </p>
     *
     * @param date the date, not altered, not null.
     * @return true if the date is today.
     * @throws IllegalArgumentException if the date is <code>null</code>
     */
    public static boolean isToday(Date date) {
        return isSameDay(date, Calendar.getInstance().getTime());
    }

    /**
     * <p>
     * Checks if a calendar date is today.
     * </p>
     *
     * @param cal the calendar, not altered, not null
     * @return true if cal date is today
     * @throws IllegalArgumentException if the calendar is <code>null</code>
     */
    public static boolean isToday(Calendar cal) {
        return isSameDay(cal, Calendar.getInstance());
    }

    /**
     * <p>
     * Checks if the first date is before the second date ignoring time.
     * </p>
     *
     * @param date1 the first date, not altered, not null
     * @param date2 the second date, not altered, not null
     * @return true if the first date day is before the second date day.
     * @throws IllegalArgumentException if the date is <code>null</code>
     */
    public static boolean isBeforeDay(Date date1, Date date2) {
        if (date1 == null || date2 == null) {
            throw new IllegalArgumentException("The dates must not be null");
        }
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(date1);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(date2);
        return isBeforeDay(cal1, cal2);
    }

    /**
     * <p>
     * Checks if the first calendar date is before the second calendar date ignoring time.
     * </p>
     *
     * @param cal1 the first calendar, not altered, not null.
     * @param cal2 the second calendar, not altered, not null.
     * @return true if cal1 date is before cal2 date ignoring time.
     * @throws IllegalArgumentException if either of the calendars are <code>null</code>
     */
    public static boolean isBeforeDay(Calendar cal1, Calendar cal2) {
        if (cal1 == null || cal2 == null) {
            throw new IllegalArgumentException("The dates must not be null");
        }
        if (cal1.get(Calendar.ERA) < cal2.get(Calendar.ERA))
            return true;
        if (cal1.get(Calendar.ERA) > cal2.get(Calendar.ERA))
            return false;
        if (cal1.get(Calendar.YEAR) < cal2.get(Calendar.YEAR))
            return true;
        if (cal1.get(Calendar.YEAR) > cal2.get(Calendar.YEAR))
            return false;
        return cal1.get(Calendar.DAY_OF_YEAR) < cal2.get(Calendar.DAY_OF_YEAR);
    }

    /**
     * <p>
     * Checks if the first date is after the second date ignoring time.
     * </p>
     *
     * @param date1 the first date, not altered, not null
     * @param date2 the second date, not altered, not null
     * @return true if the first date day is after the second date day.
     * @throws IllegalArgumentException if the date is <code>null</code>
     */
    public static boolean isAfterDay(Date date1, Date date2) {
        if (date1 == null || date2 == null) {
            throw new IllegalArgumentException("The dates must not be null");
        }
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(date1);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(date2);
        return isAfterDay(cal1, cal2);
    }

    /**
     * <p>
     * Checks if the first calendar date is after the second calendar date ignoring time.
     * </p>
     *
     * @param cal1 the first calendar, not altered, not null.
     * @param cal2 the second calendar, not altered, not null.
     * @return true if cal1 date is after cal2 date ignoring time.
     * @throws IllegalArgumentException if either of the calendars are <code>null</code>
     */
    public static boolean isAfterDay(Calendar cal1, Calendar cal2) {
        if (cal1 == null || cal2 == null) {
            throw new IllegalArgumentException("The dates must not be null");
        }
        if (cal1.get(Calendar.ERA) < cal2.get(Calendar.ERA))
            return false;
        if (cal1.get(Calendar.ERA) > cal2.get(Calendar.ERA))
            return true;
        if (cal1.get(Calendar.YEAR) < cal2.get(Calendar.YEAR))
            return false;
        if (cal1.get(Calendar.YEAR) > cal2.get(Calendar.YEAR))
            return true;
        return cal1.get(Calendar.DAY_OF_YEAR) > cal2.get(Calendar.DAY_OF_YEAR);
    }

    /**
     * <p>
     * Checks if a date is after today and within a number of days in the future.
     * </p>
     *
     * @param date the date to check, not altered, not null.
     * @param days the number of days.
     * @return true if the date day is after today and within days in the future .
     * @throws IllegalArgumentException if the date is <code>null</code>
     */
    public static boolean isWithinDaysFuture(Date date, int days) {
        if (date == null) {
            throw new IllegalArgumentException("The date must not be null");
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return isWithinDaysFuture(cal, days);
    }

    /**
     * <p>
     * Checks if a calendar date is after today and within a number of days in the future.
     * </p>
     *
     * @param cal the calendar, not altered, not null
     * @param days the number of days.
     * @return true if the calendar date day is after today and within days in the future .
     * @throws IllegalArgumentException if the calendar is <code>null</code>
     */
    public static boolean isWithinDaysFuture(Calendar cal, int days) {
        if (cal == null) {
            throw new IllegalArgumentException("The date must not be null");
        }
        Calendar today = Calendar.getInstance();
        Calendar future = Calendar.getInstance();
        future.add(Calendar.DAY_OF_YEAR, days);
        return (isAfterDay(cal, today) && !isAfterDay(cal, future));
    }

    /** Returns the given date with the time set to the start of the day. */
    public static Date getStart(Date date) {
        return clearTime(date);
    }

    /** Returns the given date with the time values cleared. */
    public static Date clearTime(Date date) {
        if (date == null) {
            return null;
        }
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTime();
    }

    /**
     * Determines whether or not a date has any time values (hour, minute, seconds or millisecondsReturns the given date with the time values cleared.
     */

    /**
     * Determines whether or not a date has any time values.
     *
     * @param date The date.
     * @return true iff the date is not null and any of the date's hour, minute, seconds or millisecond values are greater than zero.
     */
    public static boolean hasTime(Date date) {
        if (date == null) {
            return false;
        }
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        if (c.get(Calendar.HOUR_OF_DAY) > 0) {
            return true;
        }
        if (c.get(Calendar.MINUTE) > 0) {
            return true;
        }
        if (c.get(Calendar.SECOND) > 0) {
            return true;
        }
        if (c.get(Calendar.MILLISECOND) > 0) {
            return true;
        }
        return false;
    }

    /** Returns the given date with time set to the end of the day */
    public static Date getEnd(Date date) {
        if (date == null) {
            return null;
        }
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.HOUR_OF_DAY, 23);
        c.set(Calendar.MINUTE, 59);
        c.set(Calendar.SECOND, 59);
        c.set(Calendar.MILLISECOND, 999);
        return c.getTime();
    }

    /**
     * Returns the maximum of two dates. A null date is treated as being less than any non-null date.
     */
    public static Date max(Date d1, Date d2) {
        if (d1 == null && d2 == null)
            return null;
        if (d1 == null)
            return d2;
        if (d2 == null)
            return d1;
        return (d1.after(d2)) ? d1 : d2;
    }

    /**
     * Returns the minimum of two dates. A null date is treated as being greater than any non-null date.
     */
    public static Date min(Date d1, Date d2) {
        if (d1 == null && d2 == null)
            return null;
        if (d1 == null)
            return d2;
        if (d2 == null)
            return d1;
        return (d1.before(d2)) ? d1 : d2;
    }


    public static long strint2long(String date) {

        String patternbyDateStr = DatePattern.getPatternbyDateStr(date);
        if(StringUtils.isBlank(patternbyDateStr)){
            return System.currentTimeMillis();
        }
        SimpleDateFormat sdf = new SimpleDateFormat(patternbyDateStr);
        Date d = null;
        try {
            d = sdf.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
            return  System.currentTimeMillis();
        }
        return  d.getTime();

    }
    public static String add8(String date) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date parse = null;
        parse = simpleDateFormat.parse(date);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(parse);
        calendar.set(Calendar.HOUR,calendar.get(Calendar.HOUR) +8);
        
    
        return simpleDateFormat.format(calendar.getTime());
    }
    public static String timeZoneConvert_dd(String date){
        long longtime = 0L;
        SimpleDateFormat fmt2 = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        try {
            Date time = fmt2.parse(date);
            longtime = time.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return String.valueOf(longtime);
    }
    public static String timeZoneConvert_add8(String predate) {
 	/*
 	in 2023-09-04T01:37:45.606Z
 	out 2023-09-04T09:37:45.606+0800
 	 */
        
        OffsetDateTime utcDateTime = OffsetDateTime.parse(predate);
        OffsetDateTime convertedDateTime = utcDateTime.withOffsetSameInstant(ZoneOffset.ofHours(8));
        
        String outputTimestamp = convertedDateTime.format(java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        String replace = outputTimestamp.replace("+08:00", "+0800");
        return replace;
    }




    public static void main(String[] args) {
//        String predate = "12-15:17:49.575";
//        String prepattern = "yyyyMMdd-HH:mm:ss.SSS";
//        String afterpattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
//        String timezone = "Asia/Shanghai";
//        Map<String, String> result = new HashMap<>();
////        result.put("event_time","2022-12-12T15:17:49.575");
//        result.put("event_time","20221101-13:52:11.948");
////        result.put("event_time", DateUtils.timeZoneConvert(result.get("event_time"), DatePattern.getPatternbyDateStr(result.get("event_time"))));
//        System.out.println(DateUtils.timeZoneConvert(result.get("event_time"), DatePattern.getPatternbyDateStr(result.get("event_time"))));
////        System.out.println(result.toString());
////        System.out.println(ElasticSearchUtils.getIndexName(DateUtils.addHours(result.get("event_time"), DatePattern.getPatternbyDateStr(result.get("event_time"))),"yyyyMMddHH"));
////        DateUtils.convertTimeZones("Asia/Shanghai",afterpattern,"2022-09-07T09:36:51.878");
////        System.out.println(timeZoneConvert(predate, prepattern, afterpattern, timezone));
//        System.out.println( fmt2.format(System.currentTimeMillis()));
//        System.out.println(addHours("2022-12-12T15:17:49.575",afterpattern));
//        System.out.println(strint2long("2023-01-03T10:13:27.545"));
//        System.out.println(strint2long("20230103-10:13:27.048"));
//        System.out.println(strint2long("2022-11-22 10:01:03.417"));
        System.out.println(strint2long("2023-05-16 17:59:40.880"));
//        strint2long("2022-11-02 14:45:04.870");
//        strint2long("2022-11-02 14:45:04.976");
//        strint2long("2022-11-02 14:45:05.080");
//        strint2long("2022-11-02 14:45:05.064");
//        strint2long("2022-11-02 14:45:05.093");
//        strint2long("2022-11-02T14:45:05.127");
//        strint2long("2022-11-02T14:45:05.165");
//        strint2long("2022-11-02 14:45:05.192");
//        strint2long("2022-11-02 14:45:05.224");
//        strint2long("2022-11-02 14:45:05.251");
//        strint2long("2022-11-02 14:45:05.279");
//        strint2long("2022-11-02 14:45:05.293");
//        strint2long("2022-11-02 14:45:05.324");
//        System.out.println(strint2long("20230228170614898"));
//        System.out.println(getNowDay1());


    }

}
