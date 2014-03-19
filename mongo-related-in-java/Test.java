package org.basex.modules.mongodb;

import org.basex.query.*;

import com.mongodb.*;

/**
 * MongoDB test class.
 */
final class Test {
  /**
   * Test method.
   * @param args command-line arguments
   * @throws Exception exception
   */
  public static void main(final String... args) throws Exception {
    final String url = "mongodb://localhost:27017/db";
    final MongoClientURI uri = new MongoClientURI(url);

    final MongoClient client = new MongoClient(uri);

    final DB db = client.getDB(uri.getDatabase());
    if(uri.getUsername() != null && uri.getPassword() != null) {
      final boolean auth = db.authenticate(uri.getUsername(), uri.getPassword());
      if(!auth) throw new QueryException("Invalid username or password");
    }

    System.out.println("Collections:");
    for(String colName : db.getCollectionNames()) {
      System.out.println("\t + Collection: " + colName);
    } 

    db.dropDatabase();
    
    DBCollection coll = db.createCollection("one", new BasicDBObject());
    BasicDBObject career = new BasicDBObject();
    career.put("goals", 100);
    coll.insert(career);

    coll = db.createCollection("two", new BasicDBObject());
    coll.insert(career);
    client.close();
  }
}
