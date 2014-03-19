package org.basex.modules;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Set;

import org.basex.query.QueryException;
import org.basex.query.QueryModule;
import org.basex.query.func.FNJson;
import org.basex.query.func.Function;
import org.basex.query.value.Value;
import org.basex.query.value.item.Int;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.QNm;
import org.basex.query.value.item.Str;
import org.basex.query.value.map.Map;
import org.basex.query.value.type.SeqType;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

/**
 * Mongodb extension of Basex.
 *
 * @author BaseX Team 2005-13, BSD License
 * @author Prakash Thapa
 */
public class MongoDB extends QueryModule {
  /** URL of this module. */
  private static final String MONGO_URL = "http://basex.org/modules/mongodb";
  /** QName of MongoDB options. */
  private static final QNm Q_MONGODB = QNm.get("mongodb", "options", MONGO_URL);
  /** mongoclient instances. */
  private HashMap<String, MongoClient> mongoClients =
            new HashMap<String, MongoClient>();
  /** DB instances instances. */
  private HashMap<String, DB> dbs = new HashMap<String, DB>();
  /**
   * Mongodb Connection from URLstructure: mongodb://root:root@localhost/test.
   * @param url of Mongodb connection
   * @return conncetion DB connection handler of Mongodb
   * @throws QueryException
   */
  public Str connection(final Str url) throws QueryException {
        MongoClientURI uri = new MongoClientURI(url.toJava());
        String handler = "Client" + mongoClients.size();
        try {
            MongoClient mongoClient = new MongoClient(uri);
            mongoClients.put(handler, mongoClient);
            final String dbh = "DB" + dbs.size();
            try {
                DB db = mongoClient.getDB(uri.getDatabase());
                if (uri.getUsername() != null && uri.getPassword() != null) {
                    boolean auth = db.authenticate(uri.getUsername(),
                            uri.getPassword());
                    if (!auth) {
                        throw new QueryException("Invalid "
                    + "username or password");
                    }
                 }
                dbs.put(dbh, db);
                return Str.get(dbh);
                } catch (final MongoException ex) {
                    throw new QueryException(ex);
                 }
            } catch (final MongoException ex) {
                throw new QueryException(ex);
              } catch (UnknownHostException ex) {
                  throw new QueryException(ex);
              }
        }
  /**
   * Mongodb connection when provided with host port and database separately.
   *
   * @param host host name
   * @param port port
   * @param dbname name of databases
   * @return DB instance
   * @throws QueryException
   */
  public Str connection(final Str host, final Int port, final Str dbname)
      throws QueryException {
    String handler = "Client" + mongoClients.size();
    try {
      MongoClient mongoClient = new MongoClient((String) host.toJava(),
          (int) port.itr());
      mongoClients.put(handler, mongoClient);
      return Str.get(handler);

    } catch (final MongoException ex) {
      throw new QueryException(ex);
    } catch (UnknownHostException ex) {
      throw new QueryException(ex);
    }
  }
  /**
   * DB selection with mongoclient instance.
   * @param handler MongoClient handler of hashmap
   * @param dbName Databasename to connect
   * @return DB handler of hashmap
   * @throws QueryException
   */
  public Str selectDb(final Str handler, final Str dbName)
          throws QueryException {
      String ch = handler.toJava();
      final MongoClient client = mongoClients.get(ch);
      if(client == null)
          throw new QueryException("Unknown MongoDB handler: '" + ch + "'");
      final String dbh = "DB" + dbs.size();
      try {
          DB db = client.getDB(dbName.toJava());
          dbs.put(dbh, db);
          return Str.get(dbh);
      } catch (final Exception ex) {
          throw new QueryException(ex);
      }
  }
  /**
   * get DB handler from hashmap.
   * @param handler hashmap key in Str
   * @return DB handler
   * @throws QueryException
   */
  private DB getDbHandler(final Str handler) throws QueryException {
      final DB db = dbs.get(handler.toJava());
      if(db == null)
          throw new QueryException("Unknown database handler: '" +
      handler.toJava() + "'");
      return db;
    }
	/**
	 * Collection result(DBCursor) into xml item.
	 * @param result DBCursor
	 * @return Item of Xml
	 * @throws QueryException
	 */
	private Item resultToXml(final DBCursor result) throws QueryException {
	    if(result != null) {
            try {
                final Str json = Str.get(JSON.serialize(result));
                //InputInfo ii = new InputInfo(null);
                //return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
                return new FNJson(null, null, Function._JSON_PARSE, json).
                        item(context, null);
            } catch (final Exception ex) {
                throw new QueryException(ex);
            }
        } else {
          return  null;
        }
	}

	/**
	 * Collection object(DBObject) into xml item.
	 * @param object DBObject  (one row result)
	 * @return Item of Xml
	 * @throws QueryException
	 */
	private Item objectToXml(final DBObject object) throws QueryException {
	    if(object != null) {
	        try {
	            final Str json = Str.get(JSON.serialize(object));
	            //return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
	            //return new FNJson(null, Function._JSON_PARSE_PT, json).
	            //item(context, null);
	            return new FNJson(null, null, Function._JSON_PARSE, json).
	                    item(context, null);

	        } catch (final Exception ex) {
	            throw new QueryException(ex);
	        }
	    } else {
	      return  null;
	    }
	}

	/**
	 * take string as Str parameters and return DBObject of mongodb.
	 * @param string
	 * @return
	 * @throws QueryException
	 */
	private DBObject getDbObjectFromStr(final Str string) throws QueryException {
		try {
		  return  (DBObject) JSON.parse(string.toJava());
    } catch (JSONParseException e) {
      throw new QueryException("Invalid JSON syntax: " + string);
		}
	}
	/*
	public final ValueIter cnode(final Expr e) throws QueryException {
	    final SerializerOptions sopts = new SerializerOptions();
	    final ANode node =  new FNJson(null, Function._JSON_SERIALIZE, e).checkNode(e, null);
        return node.iter();
	  }
	public Str jsonNode(final Item json) {
	    if(json instanceof ANode) {
	    try {
            return (Str) new FNJson(null, Function._JSON_SERIALIZE, node).item(context, null);
        } catch (QueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	    }
//        try {
//            //Item x = new FNJson(null, Function._JSON_SERIALIZE_PT, json).item(context, null);
//        } catch (QueryException e) {
//            e.printStackTrace();
//        }
	    return null;
	}
*/
	/**
     * Return all the collections in current database.
     * @param string
     * @return result in xml element
     * @throws QueryException
     */
	public Item collections(final Str handler) throws QueryException {
	    final DB db = getDbHandler(handler);
	  Set<String> col = db.getCollectionNames();
	  BasicDBObject collection = new BasicDBObject("collections", col);
	  try {
        return objectToXml((DBObject) collection);
	  } catch (JSONParseException e) {
	      throw new QueryException("Invalid JSON syntax: " + col);
	     }
	}
	/**
	 * MongoDB find() without any attributes. eg. db.collections.find()
	 * @param handler Database handler
	 * @param col Collection name
	 * @return result in xml element
	 * @throws QueryException
	 */
	public Item find(final Str handler, final Str col) throws QueryException {
	    return find(handler, col, null, null, null);
	}
	/**
	 * MongoDB find() condtion. eg. db.collections.find({'_id':2})
	 * @param handler Database handler
	 * @param col collection
	 * @param query conditions
	 * @return xml element
	 * @throws QueryException
	 */
	public Item find(final Str handler, final Str col, final Str query)
	        throws QueryException {
	  return find(handler, col, query, null, null);
	}

  /**
   * just backup function need to remove when cleaning.
   * @param handler Database handler
   * @param col collection
   * @param query conditions
   * @param field  projection on Mongodb
   * @return xml elements
   * @throws QueryException
   */
  public Item find(final Str handler, final Str col, final Str query,
      final Map options) throws QueryException {
    return find(handler, col, query, options, null);
  }

  /**
   * just backup function need to remove when cleaning.
   * @param handler Database handler
   * @param col collection
   * @param query conditions
   * @param field  projection on Mongodb
   * @return xml elements
   * @throws QueryException
   */
  public Item find(final Str handler, final Str col, final Str query,
      final Map options, final Str projection) throws QueryException {

    final DB db = getDbHandler(handler);
    db.requestStart();
        try {
          final DBObject p = projection != null ?
              getDbObjectFromStr(projection) : null;
          final DBObject q = query != null ?
                  getDbObjectFromStr(query) : null;
          final DBCollection coll = db.getCollection(col.toJava());
          final DBCursor cursor = coll.find(q, p);
          if(options != null) {
            //final TokenMap map = new FuncParams(Q_MONGODB, null).parse(options);
            Value keys = options.keys();
            for(final Item key : keys) {
              if(!(key instanceof Str))
                  throw new QueryException("String expected, ...");
              final String k = ((Str) key).toJava();
              final Value v = options.get(key, null);
             if(v instanceof Str) {
                 if(k.equals("limit")) {
                     if(v.type().instanceOf(SeqType.ITR)) {
                         long l = ((Item) v).itr(null);
                         cursor.limit((int) l);
                     } else {
                         throw new QueryException("Invalid value...");
                     }
                 }
             } else if(v instanceof Map) {
             } else if(v instanceof Str) {
             } else {
                 throw new QueryException("Invalid value...");
             }
              if(k.equals("limit")) {
                //cursor.limit(Token.toInt(v));
              } else if(k.equals("skip")) {
                  //cursor.skip(Token.toInt(v));
              } else if(k.equals("sort")) {
                  BasicDBObject sort = new BasicDBObject(k, v);
                  sort.append("name", "-1");
                  cursor.sort((DBObject) sort);
              } else if(k.equals("explain")) {
                  cursor.explain();
                  System.out.println("explain" + v);
              }
            }

          }

          return resultToXml(cursor);
        } catch (MongoException e) {
            throw new QueryException(e.getMessage());
        } finally {
               db.requestDone();
        }
  }
	/**
	 * Mongodb's findOne() function.
	 * @param handler
	 * @param col
	 * @return
	 * @throws QueryException
	 */
	public Item findOne(final Str handler, final Str col)throws QueryException {
	    return findOne(handler, col, null, null);
	}
	/**
	 * Mongodb's findOne({'_id':2}) function with query.
	 * @param handler
	 * @param col
	 * @param query
	 * @return Item
	 * @throws QueryException
	 */
	public Item findOne(final Str handler, final Str col, final Str query)
	        throws QueryException {
	   return findOne(handler, col, query, null);
	}
	/**
	 * findOne with query projection and fields.
	 * @param handler
	 * @param col
	 * @param query
	 * @param field
	 * @return Item
	 * @throws QueryException
	 */
	public Item findOne(final Str handler, final Str col, final Str query,
	        final Str projection) throws QueryException {

	    final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            final DBObject p = projection != null ?
                    getDbObjectFromStr(projection) : null;
            final DBObject q = query != null ?
                    getDbObjectFromStr(query) : null;
            final DBCollection coll = db.getCollection(col.toJava());
            final DBObject cursor = coll.findOne(q, p);
            return  objectToXml(cursor);
        } catch (MongoException e) {
            throw new QueryException(e.getMessage());
        } finally {
               db.requestDone();
        }
	}
	/**
	 * insert data.
	 * @param handler DB Handler
	 * @param col Collection name
	 * @param insertString string to insert in json formart
	 * @throws QueryException
	 */
	public void insert(final Str handler, final Str col, final Str insertString)
	        throws QueryException {
	    final DB db = getDbHandler(handler);
		db.requestStart();
		try {
		    DBObject obj = (DBObject) JSON.parse(insertString.toJava());
	        db.getCollection(col.toJava()).insert(obj);
//	        return new FNJson(null, Function._JSON_PARSE,
//	                Str.get("Data Inserted")).item(context, null);
	   } catch (MongoException e) {
           throw new QueryException(db.getLastError().getString("err"));
        } finally {
		   db.requestDone();
		}
	}

    /**
     * Mongodb update with two parameters like update({},{}).
     * @param handler
     * @param col
     * @param insertString
     * @throws QueryException
     */
    public void update(final Str handler, final Str col, final Str query,
            final Str updatestring) throws QueryException {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            DBObject q = (DBObject) JSON.parse(query.toJava());
            DBObject o = (DBObject) JSON.parse(updatestring.toJava());
            db.getCollection(col.toJava()).update(q, o);
        } catch (MongoException e) {
            throw new QueryException(db.getLastError().getString("err"));
        } finally {
           db.requestDone();
        }
    }

    /**
     * Mongodb update with 4 parameters like update({},{}, upsert, multi).
     * @param handler Db Handler string
     * @param col Collection name
     * @param updatestring String to be updated
     * @param upsert true/false for mongodb upsert
     * @param multi true/false for mongodb multi
     * @throws QueryException
     */
    public void update(final Str handler, final Str col, final Str query,
            final Str updatestring, final boolean upsert, final boolean multi)
                    throws QueryException {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            DBObject q = (DBObject) JSON.parse(query.toJava());
            DBObject o = (DBObject) JSON.parse(updatestring.toJava());
            db.getCollection(col.toJava()).update(q, o, upsert, multi);
        } catch (MongoException e) {
            throw new QueryException(db.getLastError().getString("err"));
        } finally {
           db.requestDone();
        }
    }

    /**
	 * Mongodb Save function.
	 * @param handler DB handler
	 * @param col collection name
	 * @param saveStr string to save
	 * @throws QueryException
	 */
	public void save(final Str handler, final Str col, final Str saveStr)
	        throws QueryException {
	    final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            db.getCollection(col.toJava()).save(getDbObjectFromStr(saveStr));
            //DBObject err = db.getLastError();
        } catch (MongoException e) {
            throw new QueryException(db.getLastError().getString("err"));
        } finally {
           db.requestDone();
        }
	}

	/**
	 * Mongodb remove().
	 * @param handler
	 * @param col
	 * @param removeStr
	 * @throws QueryException
	 */
	public void remove(final Str handler, final Str col, final Str removeStr)
            throws QueryException {
	    final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            db.getCollection(col.toJava()).remove(getDbObjectFromStr(removeStr));
//            DBObject err = db.getLastError();
//            if(err != null) {
//                throw new QueryException(err.get("err").toString());
//            }
        } catch (MongoException e) {
            throw new QueryException(db.getLastError().getString("err"));
        } finally {
           db.requestDone();
        }
    }

    /**
     * Mongodb aggregate().
     * @param handler database handler
     * @param col collection name
     * @param first first aggregation compulsary
     * @return Item
     * @throws QueryException
     */
    public Item aggregate(final Str handler, final Str col, final Str first)
            throws QueryException {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
           AggregationOutput agg =  db.getCollection(col.toJava()).aggregate(
                    getDbObjectFromStr(first));
//           return new FNJson(null, Function._JSON_PARSE,
//                   Str.get(JSON.serialize(agg))).item(context, null);
           //Str.get(JSON.serialize(agg));
           for(DBObject dbObj: agg.results()) {
              return objectToXml(dbObj);
            }
        } catch (MongoException e) {
            throw new QueryException(e.getMessage());
        } finally {
           db.requestDone();
        }
        return null;
    }

    /**
     * Mongodb aggregate().
     * @param handler database handler
     * @param col collection name
     * @param first first aggregation compulsary
     * @param more more aggregation options
     * @throws QueryException
     */
    public Item aggregate(final Str handler, final Str col, final Str first,
            final Str more) throws QueryException {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            AggregationOutput agg =  db.getCollection(col.toJava()).aggregate(
                    getDbObjectFromStr(first), getDbObjectFromStr(more));
            for(DBObject dbObj: agg.results()) {
                return objectToXml(dbObj);
              }
        } catch (MongoException e) {
            throw new QueryException(e.getMessage());
        } finally {
           db.requestDone();
        }
        return  null;
    }

	/**
	 * convert json to xml.
	 * @param json
	 * @return Items in xml  format
	 * @throws QueryException
	 */
	public Item jsonToXml(final Str json) throws QueryException {
		return new FNJson(null, null, Function._JSON_PARSE, json).item(context, null);
	}

	/**
	 * take DB handler as parameter and get MongoClient and then close it.
	 * @param handler DB handler
	 * @throws QueryException
	 */
	public void close(final Str handler) throws QueryException {
		String ch = handler.toJava();
		final MongoClient client = (MongoClient) getDbHandler(handler).getMongo();
		if(client == null)
			throw new QueryException("Unknown MongoDB handler: '" + ch + "'");
		client.close();
	}
	public void  test(final Str x, final Value names) {
        for(Value s : names) {
            System.out.println(s.toString());
        }
        System.out.println(x.toJava());
    }
}
