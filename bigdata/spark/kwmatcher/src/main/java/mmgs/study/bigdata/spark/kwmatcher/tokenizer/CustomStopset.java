package mmgs.study.bigdata.spark.kwmatcher.tokenizer;

import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;

import java.util.Arrays;
import java.util.List;

public final class CustomStopset {
    public static final CharArraySet CUSTOM_STOP_WORDS_SET;

    static {
        final List<String> stopWords = Arrays.asList(
                "",
                "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                "a", "able", "about", "across", "ain", "after", "all", "almost", "also", "am", "amp", "among", "an", "and", "any", "are", "aren", "as", "at",
                "b", "be", "because", "been", "but", "by",
                "c", "can", "cannot", "class", "com", "could", "couldn", "ctr",
                "d", "dear", "did", "div", "do", "does", "didn", "doesn", "don",
                "e", "either", "else", "ever", "every",
                "f", "for", "from",
                "g", "get", "got",
                "h", "had", "has", "hasn", "have", "he", "her", "hers", "him", "his", "how", "however", "http", "https",
                "i", "id", "if", "in", "into", "is", "isn", "it", "its",
                "j", "just",
                "l", "label", "least", "let", "like", "likely", "ll", "login",
                "m", "may", "me", "might", "mightn", "most", "must", "mustn", "my",
                "n", "name", "neither", "new", "no", "nor", "not",
                "of", "off", "often", "on", "only", "option", "or", "other", "our", "out", "own",
                "p",
                "r", "rather",
                "s", "said", "say", "says", "shan", "she", "should", "shouldn", "since", "so", "some", "span",
                "t", "than", "that", "the", "their", "them", "then", "there", "these", "they", "this", "tis", "to", "too", "twas", "type",
                "us",
                "ve", "value",
                "wants", "want", "was", "wasn", "we", "were", "weren", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "would", "wouldn", "www",
                "yet", "you", "your"
        );
        final CharArraySet stopSet = new CharArraySet(stopWords, false);
        stopSet.add(StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        CUSTOM_STOP_WORDS_SET = CharArraySet.unmodifiableSet(stopSet);
    }
}