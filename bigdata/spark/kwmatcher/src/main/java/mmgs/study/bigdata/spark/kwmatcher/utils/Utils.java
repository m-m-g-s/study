package mmgs.study.bigdata.spark.kwmatcher.utils;

import mmgs.study.bigdata.spark.kwmatcher.model.WeightedKeyword;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Utils {
    public static String getTopN(List<WeightedKeyword> keywords, int n) {
        keywords.sort(Comparator.comparing(WeightedKeyword::getFrequency).reversed().thenComparing(WeightedKeyword::getKeyword));
        return keywords.stream().map(WeightedKeyword::getKeyword).distinct().limit(Math.min(n, keywords.size())).collect(Collectors.joining(" "));
    }

    private static final String DATE_FORMAT = "yyyyMMdd";
    private static final String TIMEZONE = "Etc/UTC";

    public static String getTimeLimits(String day) {
        LocalDate date = LocalDate.parse(day, DateTimeFormatter.ofPattern(DATE_FORMAT));
        ZonedDateTime zDate = date.atStartOfDay().atZone(ZoneId.of(TIMEZONE));
        return "" + zDate.minusWeeks(1).toInstant().toEpochMilli() + ',' + zDate.plusWeeks(1).toInstant().toEpochMilli();
    }

    /**
     * Initial dataset contains dates from 20130606 till 20130612. However social network crawling is possible only for days close to new.
     * Thus within this very project we have to remap dates in order to demonstrate that crawling actually works and returns data
     * @param day day in format yyyyMMdd
     * @return day in format yyyyMMdd
     */
    public static String remapDay(String day) {
        switch (day) {
            case "20130606": {return LocalDate.now().minusDays(3).format(DateTimeFormatter.ofPattern(DATE_FORMAT));}
            case "20130607": {return LocalDate.now().minusDays(2).format(DateTimeFormatter.ofPattern(DATE_FORMAT));}
            case "20130608": {return LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern(DATE_FORMAT));}
            case "20130609": {return LocalDate.now().minusDays(0).format(DateTimeFormatter.ofPattern(DATE_FORMAT));}
            case "20130610": {return LocalDate.now().plusDays(1).format(DateTimeFormatter.ofPattern(DATE_FORMAT));}
            case "20130611": {return LocalDate.now().plusDays(2).format(DateTimeFormatter.ofPattern(DATE_FORMAT));}
            case "20130612": {return LocalDate.now().plusDays(3).format(DateTimeFormatter.ofPattern(DATE_FORMAT));}
            default: {return day;}
        }
    }
}
