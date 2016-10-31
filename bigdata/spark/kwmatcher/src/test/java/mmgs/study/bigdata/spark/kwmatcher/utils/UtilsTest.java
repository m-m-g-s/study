package mmgs.study.bigdata.spark.kwmatcher.utils;

import mmgs.study.bigdata.spark.kwmatcher.model.WeightedKeyword;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class UtilsTest {
    @Test
    public void mergeKeywordsClean1() throws Exception {
        List<WeightedKeyword> kw1 = Arrays.asList(
                new WeightedKeyword("java", 10)
                , new WeightedKeyword("scala", 9)
                , new WeightedKeyword("python", 9)
                , new WeightedKeyword("hadoop", 7)
                , new WeightedKeyword("big", 7)
                , new WeightedKeyword("data", 7)
                , new WeightedKeyword("hive", 5)
                , new WeightedKeyword("spark", 5)
                , new WeightedKeyword("velocity", 2)
                , new WeightedKeyword("variety", 2)
                , new WeightedKeyword("coffee", 14)
                , new WeightedKeyword("muffin", 12)
                , new WeightedKeyword("bakery", 11)
                , new WeightedKeyword("vegan", 8)
                , new WeightedKeyword("green", 8)
                , new WeightedKeyword("soy", 7)
                , new WeightedKeyword("vegetable", 6)
                , new WeightedKeyword("vegetarian", 6)
                , new WeightedKeyword("recycle", 3)
                , new WeightedKeyword("meet", 2)
        );
        String expected = "coffee muffin bakery java python scala green vegan big data";

        String actual = Utils.getTopN(kw1, 10);
        assertEquals("Two lists without duplicates are merged correctly", expected, actual);
    }

    @Test
    public void mergeKeywordsWithDuplicates() {
        List<WeightedKeyword> kw1 = Arrays.asList(
                new WeightedKeyword("java", 10)
                , new WeightedKeyword("scala", 9)
                , new WeightedKeyword("python", 9)
                , new WeightedKeyword("hadoop", 7)
                , new WeightedKeyword("big", 7)
                , new WeightedKeyword("data", 7)
                , new WeightedKeyword("hive", 5)
                , new WeightedKeyword("spark", 5)
                , new WeightedKeyword("velocity", 2)
                , new WeightedKeyword("variety", 2)
                , new WeightedKeyword("coffee", 14)
                , new WeightedKeyword("scala", 12)
                , new WeightedKeyword("bakery", 11)
                , new WeightedKeyword("vegan", 8)
                , new WeightedKeyword("java", 8)
                , new WeightedKeyword("soy", 7)
                , new WeightedKeyword("vegetable", 6)
                , new WeightedKeyword("vegetarian", 6)
                , new WeightedKeyword("recycle", 3)
                , new WeightedKeyword("meet", 2)
        );
        String expected = "coffee scala bakery java python vegan big data hadoop soy";

        String actual = Utils.getTopN(kw1, 10);
        assertEquals("Two lists with duplicates are merged correctly", expected, actual);
    }

    @Test
    public void getTimeLimits() {
        String day = "20161024";
        String expected = "1476662400000,1477872000000";
        String actual = Utils.getTimeLimits(day);
        assertEquals("Day limit extracted correctly", actual, expected);
    }
}