package com.company;

import java.util.ArrayList;
import java.util.List;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import redis.clients.jedis.Jedis;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by mialiu on 11/27/15.
 */
public class ConnectMongoDB extends ConnectDBBase {
    private List<MongoCollection> _mongo = null;

    public boolean ConnectAllServers() {
        if (_mongo == null || _mongo.isEmpty()) {
            try {
                _mongo = new ArrayList<>(_servers.size());
                for (int i = 0; i < _servers.size(); i++) {
                    String host = _servers.get(i);

                    Logger logger = Logger.getLogger("org.mongodb.driver");
                    logger.setLevel(Level.SEVERE);
                    MongoClient client = new MongoClient(host);
                    MongoDatabase mongoDatabase = client.getDatabase("test");
                    MongoCollection mongoCollection = mongoDatabase.getCollection("collection");

                    _mongo.add(i, mongoCollection);
                }
                return true;

            } catch (Exception e) {
//                e.printStackTrace();
                _mongo.clear();
                System.out.println(e.getMessage());
                return false;
            }
        }
        return true;
    }

    public boolean Put(String key, String value) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_mongo == null || _mongo.isEmpty()) {
            return false;
        }

        int serverNumber = GetHashCode(key);
        _mongo.get(serverNumber).insertOne(new Document("key", key).append("value", value));

        return true;
    }

    public boolean Get(String key, String value) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_mongo == null || _mongo.isEmpty()) {
            return false;
        }

        int serverNumber = GetHashCode(key);
        MongoCollection coll = _mongo.get(serverNumber);

        Document condition = new Document("key", key);
        FindIterable<Document> iterable = coll.find(condition);

        final boolean[] result = {true};

        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(Document d) {
                //System.out.println(d.toString());
                result[0] = result[0] && d.getString("key").equals(key);
                result[0] = result[0] && d.getString("value").equals(value);
            }
        });

        return result[0];
    }

    public boolean Del(String key) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_mongo == null || _mongo.isEmpty()) {
            return false;
        }

        int serverNumber = GetHashCode(key);
        MongoCollection coll = _mongo.get(serverNumber);

        Document condition = new Document("key", key);
        DeleteResult result = coll.deleteOne(condition);

        long n = result.getDeletedCount();

        return (n==1);
    }
}
