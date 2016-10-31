package mmgs.study.bigdata.spark.kwmatcher.crawler;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import mmgs.study.bigdata.spark.kwmatcher.tokenizer.KeywordsExtractor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 *
 */
public abstract class MeetupCrawler implements SNCrawler, Serializable {
    private static final int SUCCESS = 200;

    protected static final String BASE_REQUEST = "https://api.meetup.com/2/";
    protected static final String RESPONSE_FORMAT = "?text_format=plain";

    protected static final Map<String, Object> QUERY_PARAMS = initQueryParams();
    protected static final String KEY_PARAM = "key";
    protected static final String TEXT_PARAM = "text";
    protected static final String LATITUDE_PARAM = "lat";
    protected static final String LONGITUDE_PARAM = "lon";

    private static Map<String, Object> initQueryParams() {
        Map<String, String> result = new HashMap<>();
        result.put("sign", "true");
        result.put("status", "upcoming");
        return Collections.unmodifiableMap(result);
    }

    protected boolean dataExtracted(HttpResponse<JsonNode> response) {
        return response.getStatus() == SUCCESS;
    }

    public List<SNItem> extractSNItems(JSONArray jsonArray) throws IOException {
        List<SNItem> snItems = new ArrayList<>();
        Iterator<Object> iterator = jsonArray.iterator();
        while (iterator.hasNext()) {
            SNItem snItem = extractSNItem((JSONObject) iterator.next());
            KeywordsExtractor.getKeywordsList(snItem.getDescription());
            snItems.add(snItem);
        }
        return Collections.unmodifiableList(snItems);
    }

    public abstract SNItem extractSNItem(JSONObject json);
}