package db;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.SortedSet;
import java.util.TreeSet;

public class MongoConnection {

    private final MongoClientURI uri = new MongoClientURI(Utils.URL_DATABASE);
    private MongoClient client;
    private MongoDatabase database;

    public SortedSet<String> getKeywords() {
    	this.openDB();
    	MongoCollection<org.bson.Document> collection = database.getCollection("departments");
    	SortedSet<String> keywordsRaw = collection.distinct("keywords", String.class).into(new TreeSet<>());
    	
    	SortedSet<String> keywords = new TreeSet<>();
    	
    	for(String i : keywordsRaw) {
    		String[] raw = i.split(",");
    		
    		for(String e : raw) {
    			if(!e.isEmpty()) {
    			
    			keywords.add(e.trim());
    			}
    		}
    		
    	}
    	client.close();
    	
    	return keywords;
    }
    	
    public void openDB() {
    	client = new MongoClient(uri);
		database = client.getDatabase("si1718-amc-departments");
    }
}
