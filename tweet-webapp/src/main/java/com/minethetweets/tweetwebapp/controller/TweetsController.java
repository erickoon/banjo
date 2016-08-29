package com.minethetweets.tweetwebapp.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.minethetweets.tweetwebapp.service.TweetQueryService;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * Created by eric on 8/28/16.
 */
@RequestMapping("/api/v1/tweets")
@RestController
public class TweetsController {

    @Autowired
    private TweetQueryService tweetQueryService;

    @RequestMapping(
            value = "/",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public List<JsonNode> list(HttpServletRequest request,
                               @RequestParam("lat") double lat,
                               @RequestParam("lon") double lon,
                               @RequestParam("rad") double radius) {


        return tweetQueryService.searchByGeoAndRadius(lon, lat, radius, "created_at", SortOrder.DESC);
    }

}
