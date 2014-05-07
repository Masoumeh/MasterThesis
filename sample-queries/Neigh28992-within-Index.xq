import module namespace geo-index = "http://expath.org/ns/GeoIndex";
import module namespace geo = "http://expath.org/ns/Geo";
declare namespace gml="http://www.opengis.net/gml";

let $a:= <gml:Polygon>
              <gml:outerBoundaryIs>
                <gml:LinearRing>
                  <gml:coordinates decimal="." cs="," ts=" ">
                  10425.2,306846.2 10425.2,621876.3 278026.1,621876.3 278026.1,306846.2 10425.2,306846.2 
                  </gml:coordinates>
                </gml:LinearRing>
              </gml:outerBoundaryIs>
            </gml:Polygon>

(:for $b in //gml:Polygon 
return if (geo:intersects( $a, $b)) then $b else ():)
return (
geo-index:filter("neighbourhoods-28992", $a)[geo:within(., $a)])
