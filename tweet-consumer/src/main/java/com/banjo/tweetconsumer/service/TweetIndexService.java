package com.banjo.tweetconsumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by eric on 8/27/16.
 */
@Service
public class TweetIndexService {

    private final static String INDEX_TWITER = "twitter";
    private final static String MAPPING_TWEET = "tweet";
    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy");

    @Value("${elastic.search.cluster}")
    private String clusterName;

    @Value("${elastic.search.host}")
    private String host;

    @Value("${elastic.search.port}")
    private int port;

    @Value("${elastic.search.delete.index}")
    private boolean deleteIndex;

    private Client client;

    private ObjectMapper mapper = new ObjectMapper();

    @PostConstruct
    public void init() throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();

        client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));

        prepareIndex();

    }

    public void indexTweet(String tweet) {

        IndexRequestBuilder indexRequestBuilder = objectBuilder(tweet);

        if(indexRequestBuilder != null) {

            IndexResponse response = indexRequestBuilder.get();

        }
    }

    private void prepareIndex() {

        if(deleteIndex && client.admin().indices().exists(new IndicesExistsRequest(INDEX_TWITER)).actionGet().isExists()) {
            
            client.admin().indices().delete(new DeleteIndexRequest(INDEX_TWITER)).actionGet();

            client.admin().indices().prepareCreate(INDEX_TWITER)
                    .addMapping(MAPPING_TWEET, "{\n" +
                            "    \"tweet\": {\n" +
                            "      \"properties\": {\n" +
                            "        \"user_id\": {\n" +
                            "          \"type\": \"string\"\n" +
                            "        },\n" +
                            "        \"screen_name\": {\n" +
                            "          \"type\": \"string\"\n" +
                            "        },\n" +
                            "        \"text\": {\n" +
                            "          \"type\": \"string\"\n" +
                            "        },\n" +
                            "        \"created_at\": {\n" +
                            "          \"type\": \"date\"\n" +
                            "        },\n" +
                            "        \"location\": {\n" +
                            "          \"type\": \"geo_point\",\n" +
                            "          \"geohash\": true,\n" +
                            "          \"geohash_prefix\": true\n" +
                            "        }\n" +
                            "      }\n" +
                            "    }\n" +
                            "  }")
                    .get();
        }
    }

    private IndexRequestBuilder objectBuilder(String tweet) {

        try {
            JsonNode jsonNode = mapper.readTree(tweet);

            if(jsonNode.has("id_str")) { //make sure its a tweet object. sometimes is a rate limit message

                Double longitude = null;
                Double latitude = null;

                if(jsonNode.hasNonNull("coordinates")) { // only index tweets that have a coordinate
                    longitude = jsonNode.get("coordinates").get("coordinates").get(0).asDouble();
                    latitude = jsonNode.get("coordinates").get("coordinates").get(1).asDouble();
                }

                String id = jsonNode.get("id_str").asText();
                String text = jsonNode.get("text").asText();
                Long userId = jsonNode.get("user").get("id").asLong();
                String screenName = jsonNode.get("user").get("screen_name").asText();
                String createdAt = jsonNode.get("created_at").asText(); //Wed Aug 27 13:08:45 +0000 2008
                Date date = DATE_FORMAT.parse(createdAt);

                if(longitude != null && latitude != null) {

                    return client.prepareIndex(INDEX_TWITER, MAPPING_TWEET, id)
                            .setSource(jsonBuilder()
                                    .startObject()
                                    .field("text", text)
                                    .field("user_id", userId)
                                    .field("screen_name", screenName)
                                    .field("created_at", date)
                                    .startArray("location").value(longitude).value(latitude).endArray()
                                    .endObject());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }


    @PreDestroy
    public void destroy(){

        client.close();
    }

}

