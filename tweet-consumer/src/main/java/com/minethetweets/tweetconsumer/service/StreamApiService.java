package com.minethetweets.tweetconsumer.service;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by eric on 8/27/16.
 */
@Component
public class StreamApiService {

    @Value("${twitter.api.consumer.key}")
    private String consumerKey;

    @Value("${twitter.api.consumer.secret}")
    private String consumerSecret;

    @Value("${twitter.api.token}")
    private String token;

    @Value("${twitter.api.secret}")
    private String secret;

    private Client client;

    private BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

    @Autowired
    private TweetIndexService tweetIndexService;

    //SF
//    private final static Location.Coordinate SW = new Location.Coordinate(-122.75,36.8);
//    private final static Location.Coordinate NE = new Location.Coordinate(-121.75,37.8);
    private final static Location.Coordinate SW = new Location.Coordinate(-180.0,-90.0);
    private final static Location.Coordinate NE = new Location.Coordinate(180.0,90.0);

    @PostConstruct
    public void init() {

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.locations(Lists.newArrayList(new Location(SW, NE)));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

        client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();
    }

    public void start() {

        client.connect();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = queue.take();
                tweetIndexService.indexTweet(msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        client.stop();

    }


}
