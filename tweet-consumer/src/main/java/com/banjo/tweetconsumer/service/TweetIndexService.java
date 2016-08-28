package com.banjo.tweetconsumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
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

    private Client client;

    private ObjectMapper mapper = new ObjectMapper();

    @PostConstruct
    public void init() throws UnknownHostException {

        // on startup

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "elasticsearch_eric").build();

        client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

    }

    public void indexTweet(String tweet) {

        try {
            JsonNode jsonNode = mapper.readTree(tweet);

            if(jsonNode.has("id_str")) {
                String id = jsonNode.get("id_str").asText();
                String text = jsonNode.get("text").asText();
                Long userId = jsonNode.get("user").get("id").asLong();
                String screenName = jsonNode.get("user").get("screen_name").asText();


                IndexResponse response = client.prepareIndex("twitter", "tweet", id)
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("text", text)
                                .field("userId", userId)
                                .field("screenName", screenName)
                                .endObject()
                        )
                        .get();

                System.out.println(response.getId());

            }


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @PreDestroy
    public void destroy(){

        client.close();
    }

}
