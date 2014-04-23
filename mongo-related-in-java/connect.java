import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;


public class connect {

	public static void main(String[] args) {
		 
	    try {
	  
		MongoClient mongo = new MongoClient("localhost", 27017);
		DB db = mongo.getDB("neighbor");
	 
		DBCollection collection = db.getCollection("neighbourhoods");
		FileInputStream fstream = new FileInputStream("/home/rostam/Desktop/Uni/master-thesis/JSON-Data/neighbourhoods-WGS-readyToUse.json");
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		String strLine;
		int count = 0;
		//Read File Line By Line
		while ((strLine = br.readLine()) != null)   {
		  // Print the content on the console
			DBObject dbObject = (DBObject)JSON.parse(strLine);
			count++;
			collection.insert(dbObject);
		}
		//Close the input stream
		br.close();
		
		
//		BasicDBObject query = new BasicDBObject("i", 71);
//
//		DBCursor cursor = collection.find(query);
//
//		try {
//		   while(cursor.hasNext()) {
//		       System.out.println(cursor.next());
//		   }
//		} finally {
//		   cursor.close();
//		}
		
		//System.out.println("Done!");
		System.out.println("no. of objects:" + count);
	    } catch (MongoException e) {
	    	e.printStackTrace();
	    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
}
