package data.streaming.test;

import java.sql.Timestamp;
import java.text.DateFormat;
import static java.util.concurrent.TimeUnit.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import db.MongoConnection;
import db.Utils;
import net.minidev.json.JSONObject;

public class Batch {
	
	private static MongoDatabase database;
	private static MongoConnection mongoConnect;
	private final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	
	
	public static void executor() {
	     final Runnable beeper = new Runnable() {
	       public void run() { 
	    	   keywordsOcurrence();
	    	   rating();
	       }
	     };
	     final ScheduledFuture<?> beeperHandle =
	       scheduler.scheduleAtFixedRate(beeper, 10, 60*5, SECONDS);
	}
	 
	
	public static void main(String... args) throws Exception {
		connect();
		executor();	
	}
	
	private static void connect() {
		MongoClientURI uri = new MongoClientURI(Utils.URL_DATABASE);
		mongoConnect = new MongoConnection();
		MongoClient client = new MongoClient(uri);
		database = client.getDatabase("si1718-amc-departments");
	}
	
	private static Integer rate(ArrayList<String> kwDepartment, ArrayList<String> kwDepartmentB){
		Integer duplicates=0;
		for (String item : kwDepartment) {
		    if (kwDepartmentB.contains(item)) {
		    	duplicates++;
		    }
		}
		return duplicates;
	}

	private static void rating() {
				
		MongoCollection<Document> collection = database.getCollection("departments");
		MongoCollection<Document> ratings = database.getCollection("ratings");
		ratings.drop();
		List<Document> documents =
				(List<Document>) collection.find().into(new ArrayList<Document>());
		
		
		JSONObject departmentRatingJSON = null;
		for (int i=0; i<documents.size()-1; i++ ) {
			Document department = documents.get(i);
			departmentRatingJSON = new JSONObject();
			JSONObject ratingJSON = new JSONObject();
			for(int a=i+1; a<documents.size(); a++) {
				
				Document departmentB = documents.get(a);
				
				Integer aux = rate((ArrayList<String>)department.get("keywords"),(ArrayList<String>)departmentB.get("keywords"));
				
				Double rate = new Double((aux * 5.0) / 4.0);
				
				ratingJSON.put(departmentB.getString("department"), rate);
			}
			departmentRatingJSON.put(department.getString("department"), ratingJSON);
			
			Document bson = org.bson.Document.parse(departmentRatingJSON.toString());
			ratings.insertOne(bson);
		}
	}

	public static void keywordsOcurrence() {
		

		SortedSet<String> keywords = mongoConnect.getKeywords();

		ArrayList<Document> kwJsonList = new ArrayList<Document>();
		MongoCollection<Document> collection = database.getCollection("tweetsDTO");
		MongoCollection<Document> collectionOcurrences = database.getCollection("tagsOcurrences");
		collectionOcurrences.drop();
				
		JSONObject keywordJSON = null;
		for (String keyword : keywords) {
			keywordJSON = new JSONObject();
			List<Document> documents =
					(List<Document>) collection.find(Filters.regex("text", ".*"+keyword+".*"))
					.into(new ArrayList<Document>());
				
			ArrayList<Object> data = new ArrayList<Object>();
			for (Document doc : documents) {
				
				//String created_at = (String) doc.get("created_at");
				DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
				Timestamp stamp = new Timestamp(Long.valueOf((String) doc.get("timestamp_ms")));
				String date = df.format(new Date(stamp.getTime()));
				//String date = created_at.substring(0, 10) + created_at.substring(25);
				
				data.add(date);
			}		
			if(!data.isEmpty()) {
				keywordJSON.put(keyword, data);
				Document bson = org.bson.Document.parse(keywordJSON.toString());
				kwJsonList.add(bson);
			}
		}
		collectionOcurrences.insertMany(kwJsonList);
		System.out.println("Completado");
	}
}
