package com.hzx.common;

import com.hzx.util.RegexUtils;

public enum DatePattern {

    // 2023-04-14:01:33:30.055
    //EEE MMM dd HH:mm:ss zzz yyyy
    //EEE MMM dd HH:mm:ss zzz yyyy
    //868 886 55 44:55:66
    //yyyy-MM-dd HH:mm:ss
    //dd MMM yyyy HH:mm:ss.SSS
    TIMESTAMP_22("yyyy-MM-dd'T'HH:mm:ss","^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}"),
    TIMESTAMP_21("yyyyMMddHHmmssSSS","^\\d{4}\\d{2}\\d{2}\\d{2}\\d{2}\\d{2}\\d{3}"),
    TIMESTAMP_20("MMddHHmmssSSS","^\\d{2}\\d{2}\\d{2}\\d{2}\\d{2}\\d{3}"),
    TIMESTAMP_19("yyyyMMddHHmmss.SSS","^\\d{4}\\d{2}\\d{2}-\\d{2}\\d{2}\\d{2}.\\d{3}"),
    TIMESTAMP_18("MMdd-HHmmss.SSS","^\\d{2}\\d{2}-\\d{2}\\d{2}\\d{2}.\\d{3}"),
    TIMESTAMP_17("yyyyMMdd:HH:mm:ss","^\\d{4}\\d{2}\\d{2}:\\d{2}:\\d{2}:\\d{2}"),
    TIMESTAMP_16("yyyy/MM/dd HH:mm:ss","^\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}"),
    TIMESTAMP_15("yyyy-MM-dd HH:mm:ss,SSS","^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}"),
    TIMESTAMP_14("dd MMM yyyy HH:mm:ss.SSS","^\\d{2} [A-Za-z]{3} \\d{4} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}"),
    TIMESTAMP_13("yyyy-MM-dd HH:mm:ss","^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"),
    TIMESTAMP_12("EEE MMM dd HH:mm:ss zzz yyyy","\\p{Alpha}{3} \\p{Alpha}{3} \\d{2} \\d{2}:\\d{2}:\\d{2} \\p{Alpha}{3} \\d{4}"),
    TIMESTAMP_11("yyyy-MM-dd'T'HH:mm:ss,SSS","^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}"),
    TIMESTAMP_10("yyyyMMddHHmmssSSS","^\\d{17}"),
    TIMESTAMP_9("yyyyMMdd:HH:mm:ss.SSS","^\\d{8}:\\d{2}:\\d{2}:\\d{2}.\\d{3}"),
    TIMESTAMP_8("yyyy-MM-dd HH:mm:ss:SSS","^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}:\\d{3}"),
    // todo: 格式需要调整
    TIMESTAMP_7("MMM  dd HH:mm:ss","^\\d{3}\\d{2}-\\d{2}"),
    TIMESTAMP_6("yyyy-MM-dd","^\\d{4}-\\d{2}-\\d{2}"),
    TIMESTAMP_5("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}Z"),
    TIMESTAMP_4("yyyy-MM-dd'T'HH:mm:ss.SSS","^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}"),
    TIMESTAMP_3("yyyy-MM-dd HH:mm:ss.SSS","^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}"),
    TIMESTAMP_2("yyyyMMdd-HH:mm:ss.SSS","^\\d{8}-\\d{2}:\\d{2}:\\d{2}.\\d{3}"),
    TIMESTAMP_1("MM-dd'T'HH:mm:ss.SSS","^\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}");

    DatePattern(String pattern, String regex){
        this.pattern = pattern;
        this.regex = regex;
    }
    private String pattern;
    private String regex;

    public String getPattern() {
        return pattern;
    }

    public String getRegex() {
        return regex;
    }

    public static String getPatternbyDateStr(String dateStr){
        for(DatePattern df:DatePattern.values()){
            if(RegexUtils.isMatch(df.getRegex(),dateStr)){
                return df.getPattern();
            }
        }

        return "";
    }
    
    public static void main(String[] args) {
        String patternbyDateStr = getPatternbyDateStr("Mon Jan 01 12:34:56 PST 2022");
        System.out.println(patternbyDateStr);
    }
}
