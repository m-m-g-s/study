package mmgs.study.bigdata.spark.kwmatcher.crawler;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import mmgs.study.bigdata.spark.kwmatcher.model.TaggedClick;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.*;

public class MeetupVenuesCrawler extends MeetupCrawler {
    private static Logger LOG = Logger.getLogger(MeetupEventsCrawler.class);

    private static final String VENUES_REQUEST = "open_venues";
    private static final Map<String, Object> FIELDS_PARAM = initFieldsParams();

    private static Map<String, Object> initFieldsParams() {
        Map<String, String> result = new HashMap<>();
        result.put("only", "id,name");
        return Collections.unmodifiableMap(result);
    }

    @Override
    public List<SNItem> extract(TaggedClick taggedClick, String connectionKey) {
        try {
            HttpResponse<JsonNode> jsonResponse = Unirest.get(BASE_REQUEST + VENUES_REQUEST + RESPONSE_FORMAT)
                    .queryString(QUERY_PARAMS)
                    .queryString(KEY_PARAM, connectionKey)
                    .queryString(TEXT_PARAM, taggedClick.getTags())
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
    private static final String NAME = "name";

    @Override
    public SNItem extractSNItem(JSONObject json) {
        return new SNItem(Integer.toString(json.getInt(ID)), json.getString(NAME));
    }
}
