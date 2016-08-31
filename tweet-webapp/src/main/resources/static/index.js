
var defaultZoom = 10;
var defaultCenter = {lat: 37.773972, lng: -122.431297};
var map;
var openedWindow = undefined;
var markers = [];

function initMap() {

    map = new google.maps.Map(document.getElementById('map'), {
        zoom: defaultZoom,
        center: defaultCenter
    });
}

function clearMarkers() {
    for (var i = 0; i < markers.length; i++) {
        markers[i].setMap(null);
    }
    markers = [];
}


function placeMarkers() {

    document.getElementById("tweets-container").innerHTML = "Tweets loading ... ";

    clearMarkers();

    var reqData = {
        "lat": parseFloat($("#lat").val()),
        "lon": parseFloat($("#lon").val()),
        "rad": parseFloat($("#rad").val()),
        "keywords": $("#keywords").val(),
    }

    map.setZoom(defaultZoom);
    map.setCenter({lat: reqData.lat, lng: reqData.lon});

    $.ajax({
        dataType: "json",
        url: "api/v1/tweets/",
        data: reqData,
        success: function( data ) {
            document.getElementById("tweets-container").innerHTML += data.length;
            var timeout = 1000;
            for (var i in data) {

                (function(tweet) {
                    var marker = new google.maps.Marker({
                        position: {lat: tweet.location[1], lng: tweet.location[0]},
                        map: map,
                        animation: google.maps.Animation.DROP,
                    });

                    var content = '@'+tweet.screen_name+'<br/>'+tweet.text;
                    marker.addListener('click', function() {
                        if(openedWindow != undefined) {
                            openedWindow.close();
                        }

                        var window = new google.maps.InfoWindow({
                                content: content
                            });

                        window.open(map, marker);
                        openedWindow = window;

                     });
                     markers.push(marker);

                     setTimeout(function() {twttr.widgets.createTweet(tweet.id, document.getElementById('tweets-container'), {width:350});}, timeout);

                     timeout += 200;

                })(data[i])


            }
        }
    });
}

$( document ).ready(function() {


    $("#button").click(function() {placeMarkers(); return false;});

});


