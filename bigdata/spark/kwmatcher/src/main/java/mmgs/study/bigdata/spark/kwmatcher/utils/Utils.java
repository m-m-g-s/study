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
        return keywords.stream().map(x -> x.getKeyword()).distinct().limit(Math.min(n, keywords.size())).collect(Collectors.joining(" "));
    }

    private static final String DATE_FORMAT = "yyyyMMdd";
    private static final String TIMEZONE = "Etc/UTC";

    public static String getTimeLimits(String day) {
        LocalDate date = LocalDate.parse(day, DateTimeFormatter.ofPattern(DATE_FORMAT));
        ZonedDateTime zDate = date.atStartOfDay().atZone(ZoneId.of(TIMEZONE));
        return "" + zDate.minusWeeks(1).toInstant().toEpochMilli() + ',' + zDate.plusWeeks(1).toInstant().toEpochMilli();

    }
}
