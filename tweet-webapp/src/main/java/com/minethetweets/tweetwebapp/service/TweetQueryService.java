package com.minethetweets.tweetwebapp.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by eric on 8/28/16.
 */
@Service
public class TweetQueryService {

    private final static String INDEX_TWITER = "twitter";
    private final static String MAPPING_TWEET = "tweet";

    @Value("${elastic.search.cluster}")
    private String clusterName;

    @Value("${elastic.search.host}")
    private String host;

    @Value("${elastic.search.port}")
    private int port;

    @Value("${elastic.search.delete.index}")
    private boolean deleteIndex;

    private Client client;

    @PostConstruct
    public void init() throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();

        client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
    }

    public List<JsonNode> searchByGeoAndRadius(double longitude, double latitude, double radiusInMiles, String keywords, String sortField, SortOrder sortOrder)  {

        QueryBuilder qb = QueryBuilders.geoHashCellQuery("location",
                new GeoPoint(latitude, longitude))
                .precision(DistanceUnit.MILES.toString(radiusInMiles));

        QueryBuilder kwQb = null;
        if(!StringUtils.isEmpty(keywords)) {
            kwQb = QueryBuilders.matchQuery("text", keywords);
        }

        SearchResponse response = client
                .prepareSearch(INDEX_TWITER)
                .setTypes(MAPPING_TWEET)
                .setQuery(qb)
                .setPostFilter(kwQb)
                .setSize(250)
                .addSort(sortField, sortOrder)
                .execute()
                .actionGet();

        List<JsonNode> jsonNodeList = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        for(SearchHit searchHit : response.getHits()) {
            try {
                JsonNode jsonNode = objectMapper.readTree(searchHit.sourceAsString());
                ((ObjectNode)jsonNode).put("id", searchHit.getId());
                jsonNodeList.add(jsonNode);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return jsonNodeList;
    }

}
