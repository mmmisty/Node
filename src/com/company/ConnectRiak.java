package com.company;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import redis.clients.jedis.Jedis;

/**
 * Created by mialiu on 11/27/15.
 */
public class ConnectRiak extends ConnectDBBase {
    private List<RiakClient> _riak = null;

    public boolean ConnectAllServers() {
        if (_riak == null || _riak.isEmpty()) {
            try {
                _riak = new ArrayList<>(_servers.size());
                for (int i = 0; i < _servers.size(); i++) {
                    String host = _servers.get(i);

                    RiakClient r1 = RiakClient.newClient(host);

                    _riak.add(i, r1);
                }
                return true;

            } catch (Exception e) {
//                e.printStackTrace();
                _riak.clear();
                System.out.println(e.getMessage());
                return false;
            }
        }
        return true;    }

    public boolean Put(String key, String value) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_riak == null || _riak.isEmpty()) {
            return false;
        }

        int serverNumber = GetHashCode(key);
        RiakClient r1 = _riak.get(serverNumber);

        Namespace ns = new Namespace("default", "my_bucket");
        Location loc = new Location(ns, key);

        RiakObject obj = new RiakObject();
        obj.setValue(BinaryValue.create(value));

        StoreValue store = new StoreValue.Builder(obj).withLocation(loc).withOption(StoreValue.Option.W, new Quorum(3)).build();
        try {
            r1.execute(store);
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    public boolean Get(String key, String value) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_riak == null || _riak.isEmpty()) {
            return false;
        }

        int serverNumber = GetHashCode(key);
        RiakClient r1 = _riak.get(serverNumber);

        Namespace ns = new Namespace("default", "my_bucket");
        Location loc = new Location(ns, key);

        FetchValue fetch = new FetchValue.Builder(loc).build();
        try {
            FetchValue.Response resp = r1.execute(fetch);
            RiakObject respObj = resp.getValue(RiakObject.class);

            RiakObject obj = new RiakObject();
            obj.setValue(BinaryValue.create(value));

            return respObj.getValue().equals(obj.getValue());
        } catch (Exception e) {
            return false;
        }
    }

    public boolean Del(String key) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_riak == null || _riak.isEmpty()) {
            return false;
        }

        int serverNumber = GetHashCode(key);
        RiakClient r1 = _riak.get(serverNumber);

        Namespace ns = new Namespace("default", "my_bucket");
        Location loc = new Location(ns, key);

        DeleteValue del = new DeleteValue.Builder(loc).build();
        try {
            r1.execute(del);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public void Close() {
        for (int i = 0; i < _riak.size(); i++) {
            RiakClient r2 = _riak.get(i);
            r2.shutdown();
        }
    }
}
