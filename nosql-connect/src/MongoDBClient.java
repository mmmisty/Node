import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Tiaotiao on 11/21/2015.
 */
public class MongoDBClient {

    public static void Test() {
        boolean ok = true;

        // disable logging
        Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
        mongoLogger.setLevel(Level.SEVERE);

        // connect
        MongoClient mongo = new MongoClient(Settings.HOST);
        MongoDatabase db = mongo.getDatabase("test");

        // get collection
        MongoCollection coll = db.getCollection("collection");

        String key = "key123";
        String val = "value456";

        // insert
        coll.insertOne(new Document("key", key).append("value", val));

        // find
        Document condition = new Document("key", key);
        FindIterable<Document> iterable = coll.find(condition);

        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(Document d) {
                //System.out.println(d.toString());
                if (!d.getString("key").equals(key)) {
                    System.out.println("MongoDB get error " + d.toString());
                }
                if (!d.getString("value").equals(val)) {
                    System.out.println("MongoDB get error " + d.toString());
                }
            }
        });

        // delete
        DeleteResult result = coll.deleteOne(condition);

        long n = result.getDeletedCount();
        if (n != 1) {
            System.out.println("MongoDB delete failed " + n);
        }

        if (ok) {
            System.out.println("MongoDB test OK");
        } else {
            System.out.println("MongoDB test FAILED");
        }
    }
}
