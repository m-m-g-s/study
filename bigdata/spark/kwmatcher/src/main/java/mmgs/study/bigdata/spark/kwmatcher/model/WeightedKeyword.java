package mmgs.study.bigdata.spark.kwmatcher.model;

import mmgs.study.bigdata.spark.kwmatcher.tokenizer.CardKeyword;

import java.io.Serializable;

/**
 * Weighted keyword
 */
public class WeightedKeyword implements Serializable {
    private String keyword;
    private long frequency;

    public WeightedKeyword(String keyword, long frequency) {
        this.keyword = keyword;
        this.frequency = frequency;
    }

    public WeightedKeyword(CardKeyword cardKeyword) {
        this.keyword = cardKeyword.getTerms().iterator().next();
        this.frequency = cardKeyword.getFrequency();
    }

    public String getKeyword() {
        return keyword;
    }

    public long getFrequency() {
        return frequency;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WeightedKeyword that = (WeightedKeyword) o;

        if (frequency != that.frequency) return false;
        return keyword.equals(that.keyword);
    }

    @Override
    public int hashCode() {
        int result = keyword.hashCode();
        result = 31 * result + (int) (frequency ^ (frequency >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "WeightedKeyword{" +
                "keyword='" + keyword + '\'' +
                ", frequency=" + frequency +
                '}';
    }
}
