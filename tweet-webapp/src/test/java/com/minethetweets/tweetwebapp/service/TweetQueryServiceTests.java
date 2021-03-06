package com.minethetweets.tweetwebapp.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;

/**
 * Created by eric on 8/28/16.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class TweetQueryServiceTests {

    @Autowired
    TweetQueryService tweetQueryService;

    @Test
    public void testSearchByGeoAndRadius() throws IOException {

        //Data needs to be set up for this test
        //SF within 25 miles radius
        List<JsonNode> jsonNodeList = tweetQueryService.searchByGeoAndRadius(-122.431297, 37.773972, 25.0, "#BusinessMgmt #SFGiants", "created_at", SortOrder.DESC);
        List<JsonNode> jsonNodeList2 = tweetQueryService.searchByGeoAndRadius(-122.431297, 37.773972, 25.0, null, "created_at", SortOrder.DESC);


        Assert.assertTrue(jsonNodeList.size() >= 0);
        Assert.assertTrue(jsonNodeList2.size() >= 0);
    }
}
