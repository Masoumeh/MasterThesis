package org.basex.modules;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import net.spy.memcached.internal.OperationFuture;

import org.basex.query.QueryException;
import org.basex.query.QueryModule;
import org.basex.query.func.FNJson;
import org.basex.query.func.Function;
import org.basex.query.value.Value;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.Str;
import org.basex.query.value.map.Map;
import org.basex.query.value.type.SeqType;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewDesign;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;


/**
 * CouchBase extension of Basex.
 *
 * @author BaseX Team 2005-13, BSD License
 * @author Prakash Thapa
 */
public class Couchbase extends QueryModule {
  /** URL of this module. */
 //private static final String COUCHBASE_URL = "http://basex.org/modules/couchbase";
  /** QName of Couchbase options. */
  //private static final QNm Q_COUCHBASE = QNm.get("couchbase", "options", COUCHBASE_URL);
  private HashMap<String, CouchbaseClient> couchbaseclients =
            new HashMap<String, CouchbaseClient>();
    //private ArrayList<URI> nodes = new ArrayList<URI>();
    public Str connection(final Str url, final Str bucket, final Str password)
            throws Exception {
//        String handler = "Client" + couchbaseclients.size();
//        System.out.println(handler);
//        return Str.get(handler);
     try {
         String handler = "Client" + couchbaseclients.size();
         List<URI> hosts = Arrays.asList(new URI(
                 url.toJava())
         );
//         CouchbaseConnectionFactory cf = new CouchbaseConnectionFactory(
//                 hosts, bucket.toJava(), password.toJava());
//          CouchbaseClient client    =   new CouchbaseClient(cf);
            CouchbaseClient client = new CouchbaseClient(hosts,
          bucket.toJava(), password.toJava());
          couchbaseclients.put(handler, client);
          return Str.get(handler);
          } catch (Exception ex) {
              throw new QueryException(ex);
          }
    }
    /**
     * get CouchbaseClinet from the hashmap.
     * @param handler
     * @return
     * @throws QueryException
     */
    private CouchbaseClient getClient(final Str handler) throws QueryException {
        String ch = handler.toJava();
        try {
            final CouchbaseClient client = couchbaseclients.get(ch);
            if(client == null)
                throw new QueryException("Unknown CouchbaseClient handler: '" + ch + "'");
            return client;

        } catch (final Exception ex) {
            throw new QueryException(ex);
        }
    }
    /**
     * Check if the string is json format or not. if not throw exception
     * @param doc
     * @throws QueryException
     */
    private void checkJson(final Str doc) throws QueryException {
        try {
            new FNJson(null, null, Function._JSON_PARSE,
                    doc).item(context, null);
        } catch (Exception e) {
            throw new QueryException("document is not in json format");
        }
    }
    /**
     * add new document.
     * @param handler Database handler
     * @param key
     * @param value
     * @throws QueryException
     */
    public Item add(final Str handler, final Str key, final Str doc)
            throws QueryException {
        return put(handler, key, doc, "add");
    }
    /**
     * 
     * @param handler
     * @param key
     * @param doc
     * @return
     * @throws QueryException
     */
    public Item set(final Str handler, final Str key,  final Str doc)
            throws QueryException {
        return put(handler, key, doc, "set");
    }
    /**
     * 
     * @param handler
     * @param key
     * @param doc
     * @return
     * @throws QueryException
     */
    public Item replace(final Str handler, final Str key, final Str doc)
            throws QueryException {
       return put(handler, key, doc, "replace");
    }
    /**
     * 
     * @param handler
     * @param key
     * @param doc
     * @return
     * @throws QueryException
     */
    public Item append(final Str handler, final Str key, final Str doc)
            throws QueryException {
       return put(handler, key, doc, null);
    }
    /**
     * 
     * @param handler
     * @param key
     * @param doc
     * @param options
     * @return 
     * @throws QueryException
     */
    public Item put(final Str handler, final Str key, final Str doc,
            final String type) throws QueryException {
        CouchbaseClient client = getClient(handler);
        OperationFuture<Boolean> result = null;
       checkJson(doc);
        try {
//          OperationFuture<Boolean> appendResult = client.append(
//            key.toJava(), doc.toJava());
//          if(options != null) {
//              Value keys = options.keys();
//              for(final Item mapKey : keys) {
//                  if(!(key instanceof Str))
//                      throw new QueryException("String expected, ...");
//                  final String k = ((Str) mapKey).toJava();
//                  final Value v = options.get(key, null);
//                  if(k.equals("type")){
//                      if(v.type().instanceOf(SeqType.STR)) {
//                          System.out.println(v.toJava());
//                      }
//                  }
//              }
//          } else {
//          }
            if(type != null) {
                if(type.equals("add")) {
                   client.add(
                            key.toJava(), doc.toJava());
                } else if(type.equals("replace")) {
                   client.replace(
                            key.toJava(), doc.toJava());
                } else if(type.equals("set")) {
                   result = client.set(
                            key.toJava(), doc.toJava());
                } else {
                   result = client.append(
                            key.toJava(), doc.toJava());
                }
            } else {
                result = client.append(
                        key.toJava(), doc.toJava());
            }
            String msg = result.getStatus().getMessage();
            if(result.get().booleanValue()) {
                //query successfully executed
                //return resultItem(result);
                return Str.get(msg);
            } else {
                throw new QueryException("operation fail " + msg);
            }
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
        //return null;
    }
    public Item delete(final Str handler, final Str key) throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
            OperationFuture<Boolean> result = client.delete(key.toJava());
            String msg = result.getStatus().getMessage();
            if(result.get().booleanValue()) {
                return Str.get(msg);
            } else {
                throw new QueryException("operation fail:" + msg);
            }
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
    }
    public Item get(final Str handler, final Str key) throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
            Object result =  client.get(key.toJava());
            if(result != null) {
                 Str json = Str.get((String) result);
                try {
                    return new FNJson(null, null, Function._JSON_PARSE,
                            json).item(context, null);
                } catch (Exception e) {
                    throw new QueryException("The result is not in json Format");
                }
            } else
              throw new QueryException("Element is empty");
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
    }
    /**
     * 
     * @param handler
     * @param doc
     * @param options
     * @return
     * @throws QueryException
     */
    public Str get(final Str handler, final Str doc, final Map options)
         throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
            if(options != null) {
                Value keys = options.keys();
                for(final Item key : keys) {
                    if(!(key instanceof Str))
                        throw new QueryException("String expected, ...");
                    final String k = ((Str) key).toJava();
                    final Value v = options.get(key, null);
                    if(k.equals("add")) {
                        if(v.type().instanceOf(SeqType.STR)) {
                            Str s = (Str) v.toJava();
                            return s;
                        }
                    }
                }
            }
            final Object o = client.get(doc.toJava());
            if(o != null) {
             //return get(client, doc);
                return Str.get(o.toString());
            } else
                throw new QueryException("Element is empty");
            //return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
    }
    public Item getbulk(final Str handler, final Value options) throws QueryException {
         //CouchbaseClient client = getClient(handler);
         try {
             if(options.size() < 1) {
                 throw new QueryException("key set is empty");
             }
             List<String> keys = new ArrayList<String>();
             for (Value v: options) {
                String s = (String) v.toJava();
                keys.add(s);
             }
             //java.util.Map<String, Object> s = client.getBulk(keys);
             //Object s = client.getBulk(keys);
             //return new FNJson(null, Function._JSON_PARSE, Str.get(json))
             //.item(context, null);
         } catch (Exception ex) {
            throw new QueryException(ex);
        }
        return handler;
    }
    /**
     * remove document by key.
     * @param handler
     * @param key
     * @return
     * @throws QueryException
     */
    public Item remove(final Str handler, final Str key) throws QueryException {
        return delete(handler, key);
    }
    /**
     * insert Binary Object in database.
     * @param handler
     * @param docbin
     * @return
     * @throws QueryException
     */
    public Item getBinary(final Str handler, final Str docbin) throws QueryException {
        CouchbaseClient client = getClient(handler);
        client.get("s");
      return null;
    }
    public Item putText(final Str handler, final Str key, final Str value) {
        return null;
    }
    public Item putBinary(final Str handler, final Str key, final Str value) {
        return null;
    }
    /**
     * Create view without reduce method.
     * @param handler
     * @param doc
     * @param viewName
     * @param map
     * @return
     * @throws QueryException
     */
    public Item createView(final Str handler, final Str doc, final Str viewName,
            final Str map) throws QueryException {
        return createView(handler, doc, viewName, map, null);
    }
    /**
     * Create view with reduce method.
     * @param handler Database handler
     * @param doc
     * @param viewName
     * @param map
     * @param reduce
     * @return
     * @throws QueryException
     */
    @SuppressWarnings("rawtypes")
    public Item createView(final Str handler, final Str doc, final Str viewName,
            final Str map, final Str reduce) throws QueryException {
        CouchbaseClient client = getClient(handler);
        if(map == null) {
            throw new QueryException("map function is empty");
        }
        try {
            DesignDocument<?> designDoc = new DesignDocument(doc.toJava());
            ViewDesign viewDesign;
            if(reduce != null) {
               viewDesign = new ViewDesign(viewName.toJava(),
                       map.toJava(), reduce.toJava());
            } else {
                viewDesign = new ViewDesign(viewName.toJava(), map.toJava());
            }
            designDoc.getViews().add(viewDesign);
            client.createDesignDoc(designDoc);
        } catch (Exception e) {
            throw new QueryException(e);
        }
        return null;
    }
    /**
     * 
     * @param handler
     * @param doc
     * @param viewName
     * @return
     * @throws QueryException
     */
    public Item getview(final Str handler, final Str doc, final Str viewName)
            throws QueryException {
        return getview(handler, doc, viewName, null, null);
    }
    /**
     * Get data from view.
     * @param handler
     * @param doc
     * @param viewName
     * @param mode is development or production
     * @return
     * @throws QueryException
     */
    public Item getview(final Str handler, final Str doc, final Str viewName,
            final Str mode)
            throws QueryException {
        return getview(handler, doc, viewName, mode, null);
    }
    /**
     * 
     * @param handler
     * @param doc
     * @param viewName
     * @param mode
     * @param options options like limit and so on(not completed)
     * @return
     * @throws QueryException
     */
    public Item getview(final Str handler, final Str doc, final Str viewName,
            final Str mode, final Str options) throws QueryException {
        if(mode != null) {
            System.setProperty("viewmode", mode.toJava());
        }
        CouchbaseClient client = getClient(handler);
        try {
            View view = client.getView(doc.toJava(), viewName.toJava());
            Query q = new Query();
            q.setIncludeDocs(true).setLimit(10);
            q.setStale(Stale.FALSE);
            ViewResponse response = client.query(view, q);
            java.util.Map<String, Object> map = response.getMap();
//            for(ViewRow row: response) {
//                System.out.println(row.getDocument());
//            }
            Str json = mapToJson(map);
            return new FNJson(null, null, Function._JSON_PARSE, json).
                    item(context, null);
        } catch (Exception e) {
            throw new QueryException(e);
        }
        //return null;
    }
    /**
     * Convert java Map<String, Object> into json format String and then return
     * in Str.
     * @param map
     * @return
     */
    private Str mapToJson(final java.util.Map<String, Object> map) {
        final StringBuilder json = new StringBuilder();
        json.append("{ ");
        for(final Entry<String, Object> e : map.entrySet()) {
          if(json.length() > 2) json.append(", ");
          json.append('"').append(e.getKey()).append('"').append(" : ").
          append(e.getValue());
        }
        json.append(" } ");
    return Str.get(json.toString());
    }
    /**
     *  close database instanses.
     * @param handler
     * @throws QueryException
     */
    public void shutdown(final Str handler) throws QueryException {
        CouchbaseClient client = getClient(handler);
        client.shutdown();
    }
    /****** testing methods.
     * @throws QueryException *************************/
    public Str test(final Map options) throws QueryException {
        if(options != null) {
            Value keys = options.keys();
            for(final Item key : keys) {
                if(!(key instanceof Str))
                    throw new QueryException("String expected, ...");
                final String k = ((Str) key).toJava();
                final Value v = options.get(key, null);
                if(k.equals("type")) {
                    return (Str) v;
                }
            }
        }
        return null;
    }
    public Item showviews(final Str handler) throws QueryException {
        CouchbaseClient client = getClient(handler);
        View view = client.getView("beer", "brewery_beers");
        Query query = new Query();
        query.setIncludeDocs(true).setLimit(5); // include all docs and limit to 5
        ViewResponse result = client.query(view, query);

        // Iterate over the result and print the key of each document:
       //create json string with key and value
        String json = "";
        for(ViewRow row : result) {
            final String k = row.getKey();
            final String v = row.getValue();
            json = json + "{" + k + ":" + v + "},";
          // The full document (as String) is available through row.getDocument();
        }
        return new FNJson(null, null, Function._JSON_PARSE, Str.get(json)).
                item(context, null);
    }
    //tests
    public Str getView(final Str handler) throws QueryException {
        CouchbaseClient client = getClient(handler);
        View view = client.getView("Beer", "brewery_beers");
        Query query = new Query();
        ViewResponse result = client.query(view, query);
        Str json = Str.get(result.toString());
        return json;
    }
}
