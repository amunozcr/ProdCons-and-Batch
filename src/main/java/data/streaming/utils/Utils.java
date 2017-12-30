package data.streaming.utils;

import java.io.IOException;

import org.bson.Document;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import data.streaming.dto.TweetDTO;

public class Utils {
	
	
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final MongoClientURI uri = new MongoClientURI("mongodb://alex:alex@ds151355.mlab.com:51355/si1718-amc-departments");
	private static final MongoClient client = new MongoClient(uri);
	private static final MongoDatabase db = client.getDatabase(uri.getDatabase());
	

	public static TweetDTO createTweetDTO(String x) {
		TweetDTO result = null;

		try {
			result = mapper.readValue(x, TweetDTO.class);
		} catch (IOException e) {

		}
		return result;
	}
	
	// TODO Auto-generated method stub
	public static boolean isValid(String str) {
		
		boolean res = true;
		
		if(createTweetDTO(str) == null) {
			res = false;
		}
		return res;
	}
	
	public static boolean saveTweet(Document doc) {

	    MongoCollection collection = db.getCollection("tweetsDTO");
	    Document doc_i = new Document();
    
	    doc_i.append("created_at", doc.getString("created_at"));
	    doc_i.append("text", doc.getString("text"));
	    doc_i.append("source", doc.getString("source"));
	    doc_i.append("timestamp_ms", doc.getString("timestamp_ms"));
	    
		collection.insertOne(doc_i);
		
		return true;
	}

}
