import module namespace mongo = 'http://basex.org/modules/mongodb/MongoDB';

let $mongodb := mongo:connect("localhost", 12345)
return (

  let $gml := <gml xmlns:gml="http://www.opengis.net/gml">
    <gml:Polygon>
       <gml:outerBoundaryIs>
         <gml:LinearRing>
           <gml:posList>0,0 100,0 100,100 0,100 0,0</gml:posList>
         </gml:LinearRing>
      </gml:outerBoundaryIs>
    </gml:Polygon>
  </gml>
  
  for $polygon at $pos in $gml//*:Polygon
  let $xml := <json type='object'>
    <type>Feature</type>
    <geometry type="object">
      <type>Polygon</type>
      <coordinates type="array">
        <_ type="array">{
          let $list := $polygon//*:posList/string()
          for $coord in tokenize($list, ' +')
          let $xy := tokenize($coord, ',')
          return <_ type="array">
            <_ type="number">{ $xy[1] }</_>
            <_ type="number">{ $xy[2] }</_>
          </_>
        }</_>
      </coordinates>
    </geometry>
  </json>
  
  return mongo:insert($mongodb, $xml),

  mongo:close($mongodb)
)

