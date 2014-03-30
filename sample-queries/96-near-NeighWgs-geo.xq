import module namespace geo = "http://expath.org/ns/Geo";
declare namespace gml="http://www.opengis.net/gml";

let $a:= <gml:Point>
              
                  <gml:coordinates decimal="." cs="," ts=" ">
               4.5,51.95 
                  </gml:coordinates>
                
            </gml:Point>

for $b in //gml:Polygon 
return if (geo:distance( $a, $b) le 0.055) then $b else ()
