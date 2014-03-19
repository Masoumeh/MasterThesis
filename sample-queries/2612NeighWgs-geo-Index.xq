import module namespace geo-index = "http://expath.org/ns/GeoIndex";
import module namespace geo = "http://expath.org/ns/Geo";
declare namespace gml="http://www.opengis.net/gml";

let $a:= <gml:Polygon>
              <gml:outerBoundaryIs>
                <gml:LinearRing>
                  <gml:coordinates decimal="." cs="," ts=" ">
                   3.9,50.6 6,51.95 6,52.8 4.5,52.8 3.9,50.6
                  </gml:coordinates>
                </gml:LinearRing>
              </gml:outerBoundaryIs>
            </gml:Polygon>

(:for $b in //gml:Polygon 
return if (geo:intersects( $a, $b)) then $b else ():)
return (
geo-index:filter("neighbourhoods-WGS", $a)[geo:intersects( $a, .)])
