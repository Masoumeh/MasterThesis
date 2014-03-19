import module namespace geo = "http://expath.org/ns/Geo";
declare namespace gml="http://www.opengis.net/gml";

let $a:= <gml:Polygon>
              <gml:outerBoundaryIs>
                <gml:LinearRing>
                  <gml:coordinates decimal="." cs="," ts=" ">
                   3,50.6 6.6,51.95 6.6,52.8 4.2,52.8 3,50.6
                  </gml:coordinates>
                </gml:LinearRing>
              </gml:outerBoundaryIs>
            </gml:Polygon>

for $b in //gml:Polygon 
return if (geo:intersects( $a, $b)) then $b else ()
