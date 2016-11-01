package mmgs.study.bigdata.spark.kwmatcher.crawler;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import mmgs.study.bigdata.spark.kwmatcher.model.TaggedClick;
import mmgs.study.bigdata.spark.kwmatcher.utils.Utils;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MeetupEventsCrawler extends MeetupCrawler {
    private static final String EVENTS_REQUEST = "open_events";
    private static final String TIME_PARAM = "time";
    private static final Map<String, Object> FIELDS_PARAM = initFieldsParams();

    private static Map<String, Object> initFieldsParams() {
        Map<String, String> result = new HashMap<>();
        result.put("only", "id,description");
        return Collections.unmodifiableMap(result);
    }

    @Override
    public List<SNItem> extract(TaggedClick taggedClick, String connectionKey) throws Exception {
        HttpResponse<JsonNode> jsonResponse = Unirest.get(BASE_REQUEST + EVENTS_REQUEST + RESPONSE_FORMAT)
                .queryString(QUERY_PARAMS)
                .queryString(KEY_PARAM, connectionKey)
                .queryString(TEXT_PARAM, taggedClick.getTags())
                .queryString(TIME_PARAM, Utils.getTimeLimits(taggedClick.getDay()))
                .queryString(LATITUDE_PARAM, Double.toString(taggedClick.getLatitude()))
                .queryString(LONGITUDE_PARAM, Double.toString(taggedClick.getLongitude()))
                .queryString(FIELDS_PARAM)
                .asJson();

        if (dataExtracted(jsonResponse)) {
            return extractSNItems(jsonResponse.getBody().getObject().getJSONArray("results"));
        } else {
            // TODO: handle exceptions properly
            System.out.println(jsonResponse.getStatus());
            throw new Exception("Something went wrong");
        }
    }

    private static final String ID = "id";
    private static final String DESCRIPTION = "description";

    @Override
    public SNItem extractSNItem(JSONObject json) {
        return new SNItem(json.getString(ID), json.getString(DESCRIPTION));
    }
}
