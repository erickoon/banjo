package com.banjo.tweetconsumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by eric on 8/27/16.
 */
@Service
public class TweetIndexService {

    private final static String INDEX_TWITER = "twitter";
    private final static String MAPPING_TWEET = "tweet";

    @Value("${elastic.search.cluster}")
    private String clusterName;

    @Value("${elastic.search.host}")
    private String host;

    @Value("${elastic.search.port}")
    private int port;

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

        JsonNode jsonNode = toJsonNode(tweet);

        if(jsonNode != null) {

            if(jsonNode.hasNonNull("coordinates")) {
                System.out.println("hascoordinates");
            }
            else if(jsonNode.hasNonNull("place")) {
                System.out.println("hasplaces");

                //https://dev.twitter.com/overview/api/places
            }
            String id = jsonNode.get("id_str").asText();
            String text = jsonNode.get("text").asText();
            Long userId = jsonNode.get("user").get("id").asLong();
            String screenName = jsonNode.get("user").get("screen_name").asText();

            /**
             * "coordinates":
             {
             "coordinates":
             [
             -75.14310264,
             40.05701649
             ],
             "type":"Point"
             }
             */

            IndexResponse response = null;
            try {
                response = client.prepareIndex(INDEX_TWITER, MAPPING_TWEET, id)
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("text", text)
                                .field("user_id", userId)
                                .field("screen_name", screenName)
                                .endObject()
                        )
                        .get();

                System.out.println(response.getId());

            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    private void prepareIndex() {

        if(client.admin().indices().exists(new IndicesExistsRequest(INDEX_TWITER)).actionGet().isExists()) {
            client.admin().indices().delete(new DeleteIndexRequest(INDEX_TWITER)).actionGet(); // start fresh
        }

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

    private JsonNode toJsonNode(String tweet) {

        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(tweet);

        } catch (IOException e) {
            e.printStackTrace();
        }

        if(jsonNode.has("id_str")) {
            return jsonNode;
        }

        return null;
    }

    @PreDestroy
    public void destroy(){

        client.close();
    }

}

