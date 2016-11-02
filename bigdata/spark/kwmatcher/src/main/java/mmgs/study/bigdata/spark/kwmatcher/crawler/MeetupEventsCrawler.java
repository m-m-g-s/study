package mmgs.study.bigdata.spark.kwmatcher.crawler;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import mmgs.study.bigdata.spark.kwmatcher.model.TaggedClick;
import mmgs.study.bigdata.spark.kwmatcher.utils.Utils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

public class MeetupEventsCrawler extends MeetupCrawler {
    private static Logger LOG = Logger.getLogger(MeetupEventsCrawler.class);

    private static final String EVENTS_REQUEST = "open_events";
    private static final String TIME_PARAM = "time";
    private static final Map<String, Object> FIELDS_PARAM = initFieldsParams();

    private static Map<String, Object> initFieldsParams() {
        Map<String, String> result = new HashMap<>();
        result.put("only", "id,name,description");
        return Collections.unmodifiableMap(result);
    }

    @Override
    public List<SNItem> extract(TaggedClick taggedClick, String connectionKey) {
        try {
            HttpResponse<JsonNode> jsonResponse = Unirest.get(BASE_REQUEST + EVENTS_REQUEST + RESPONSE_FORMAT)
                    .queryString(QUERY_PARAMS)
                    .queryString(KEY_PARAM, connectionKey)
                    .queryString(TEXT_PARAM, taggedClick.getTags())
                    .queryString(TIME_PARAM, Utils.getTimeLimits(taggedClick.getDay()))
                    .queryString(LATITUDE_PARAM, Double.toString(taggedClick.getLatitude()))
                    .queryString(LONGITUDE_PARAM, Double.toString(taggedClick.getLongitude()))
                    .queryString(FIELDS_PARAM)
                    .asJson();

            return extractSNItems(jsonResponse);
        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    private static final String ID = "id";
    private static final String DESCRIPTION = "description";
    private static final String NAME = "name";

    @Override
    public SNItem extractSNItem(JSONObject json) {
        String desc;
        try {
            desc = json.getString(DESCRIPTION);
        } catch (JSONException e) {
            // it's ok for an event not to have a description
            desc = "";
        }
        return new SNItem(json.getString(ID), json.getString(NAME) + " " + desc);
    }
}
