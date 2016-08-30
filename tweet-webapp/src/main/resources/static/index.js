
var map;
var openedWindow = undefined;
var markers = [];

function initMap() {

    map = new google.maps.Map(document.getElementById('map'), {
        zoom: 10,
        center: {lat: 37.773972, lng: -122.431297}
    });
}

function clearMarkers() {
    for (var i = 0; i < markers.length; i++) {
        markers[i].setMap(null);
    }
    markers = [];
}


function placeMarkers() {

    clearMarkers();

    var reqData = {
        "lat": parseFloat($("#lat").val()),
        "lon": parseFloat($("#lon").val()),
        "rad": parseFloat($("#rad").val()),
        "keywords": $("#keywords").val(),
    }

    map.setCenter({lat: reqData.lat, lng: reqData.lon});

    $.ajax({
        dataType: "json",
        url: "api/v1/tweets/",
        data: reqData,
        success: function( data ) {

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
                })(data[i])


            }
        }
    });

//    $.getJSON( "api/v1/tweets/?lat=37.773972&lon=-122.431297&rad=50", function( data ) {
//
//        for (var i in data) {
//
//            (function(tweet) {
//                var marker = new google.maps.Marker({
//                    position: {lat: tweet.location[1], lng: tweet.location[0]},
//                    map: map,
//                    animation: google.maps.Animation.DROP,
//                });
//
//                var content = '@'+tweet.screen_name+'<br/>'+tweet.text;
//                marker.addListener('click', function() {
//                    if(openedWindow != undefined) {
//                        openedWindow.close();
//                    }
//
//                    var window = new google.maps.InfoWindow({
//                            content: content
//                        });
//
//                    window.open(map, marker);
//                    openedWindow = window;
//
//                 });
//            })(data[i])
//
//
//        }
//    });
}

$( document ).ready(function() {


    $("#button").click(function() {placeMarkers(); return false;});

});


