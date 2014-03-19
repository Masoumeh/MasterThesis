import module namespace geo = "http://expath.org/ns/Geo";
declare namespace gml="http://www.opengis.net/gml";

let $a:= <gml:Polygon>
              <gml:outerBoundaryIs>
                <gml:LinearRing>
                  <gml:coordinates decimal="." cs="," ts=" ">
                5.5,51.1 6,51.1 6,52.30 5.5,52.30 5.5,51.1
                  </gml:coordinates>
                </gml:LinearRing>
              </gml:outerBoundaryIs>
            </gml:Polygon>

for $b in //gml:Polygon 
return if (geo:intersects( $a, $b)) then $b else ()
