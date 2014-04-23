#! /bin/awk -f

BEGIN {FS = ",|\t"; 
       counter = 0;
       // init color map
       colors[0]="#00FFFF";	#black
       colors[1]="#000000";	#blue
       colors[2]="#0000FF";	#fuchsia
       colors[3]="#FF00FF";    #gray
       colors[4]="#808080";	#green
       colors[5]="#008000";	#lime
       colors[6]="#00FF00";	#maroon
       colors[7]="#800000";     #navy
       colors[8]="#000080";	#olive
       colors[9]="#808000";	#purple
       colors[10]="#800080";    #red
       colors[11]="#FF0000";     #silver
       colors[12]="#C0C0C0";	#teal
       colors[13]="#008080";	#white
       colors[14]="#FFFFFF";	#yellow
       colors[15]="#FFFF00";
       
print "<!DOCTYPE html>"
print "<html>"
print "  <head>  "
print "    <meta name=\"viewport\" content=\"initial-scale=1.0, user-scalable=no\" />"
print "    <style type=\"text/css\">"
print "      html { height: 100% }"
print "     body { height: 100%; margin: 0; padding: 0 }"
print "      #map-canvas { height: 100% }"
print "    </style>"
print "    <script src=\"http://maps.google.com/maps?file=js\" type=\"text/javascript\"></script>"
print "    <script type=\"text/javascript\""
print "      src=\"https://maps.googleapis.com/maps/api/js?v=3key={AIzaSyCgOMjhFBXtFwIWzgvhlJ5guikMLZm6YHs}&sensor=false\">"
print "    </script>"
print "    <script type=\"text/javascript\">"
print "" 
print "     // Add all clusters here"
print "     var clusters = {};"

}

{ 
  print "clusters['" counter "'] = { center: new google.maps.LatLng(" $3 "," $4 "), clusterId:" counter ", size:300, color: '" colors[$1] "'};"
  counter = counter + 1;
}

END {
print "     var clusterCircle;"
print "     //Add all businesses here"
print "     //var businessVec = {};"
print "     //businessVec['1'] = { center : new google.maps.LatLng(33.45, -112.066), clusterId: 1, name: 'SunnyD'};"
print "     //businessVec['2'] = { center : new google.maps.LatLng(33.9, -112.3), clusterId: 2, name:'YusufElGalaind'};"
print "     //var businessMarker;"
print "          "
print "     function initialize() {"
print "       var mapOptions = { center: new google.maps.LatLng(33.4500, -112.0667), zoom: 10 };"
print "       var map = new google.maps.Map(document.getElementById(\"map-canvas\"), mapOptions);"
print ""
print "       // Add all cluster circles"
print "       for (var cluster in clusters) {"
print "         var options = {"
print "           strokeColor: clusters[cluster].color,"
print "           strokeOpacity: 0.8,"
print "           strokeWeight: 2,"
print "           fillColor: clusters[cluster].color,"
print "           fillOpacity: 0.35,"
print "           map: map,"
print "           center: clusters[cluster].center,"
print "           radius: clusters[cluster].size"
print "         };"
print "         // Add the circle for this city to the map."
print "         cluterCircle = new google.maps.Circle(options);"
print "       }"
print "            "
print "       // Add all business markers"
print "       //for (var business in businessVec) {"
print "       //  var marker = new google.maps.Marker({"
print "       //    position: businessVec[business].center,"
print "       //    map: map,"
print "       //    title: businessVec[business].name"
print "       //  });             "
print "       //}"
print "     }"
print "     google.maps.event.addDomListener(window, 'load', initialize);"
print "    </script>"
print "  </head>"
print "  <body>"
print "    <div id=\"map-canvas\"/>"
print "  </body>"
print "</html>"


}
