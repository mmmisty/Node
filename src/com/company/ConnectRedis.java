package com.company;
import redis.clients.jedis.Jedis;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mialiu on 11/27/15.
 */
public class ConnectRedis extends ConnectDBBase {
    private List<Jedis> _redis = null;

    public ConnectRedis() {

    }

    public boolean ConnectAllServers() {
        if (_redis == null || _redis.isEmpty()) {
            try {
                _redis = new ArrayList<>(_servers.size());
                for (int i = 0; i < _servers.size(); i++) {
                    String host = _servers.get(i);

                    Jedis j1 = new Jedis(host);
                    j1.configSet("stop-writes-on-bgsave-error", "no");

                    _redis.add(i, j1);
                }
                return true;

            } catch (Exception e) {
//                e.printStackTrace();
                _redis.clear();
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
        if (_redis == null || _redis.isEmpty()) {
            return false;
        }

        int serverNumber = GetHashCode(key);
        _redis.get(serverNumber).set(key, value);

        return true;
    }

    public boolean Get(String key, String value) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_redis == null || _redis.isEmpty()) {
            return false;
        }

        int serverNumber = GetHashCode(key);
        String getval = _redis.get(serverNumber).get(key);

        return getval.equals(value);
    }

    public boolean Del(String key) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_redis == null || _redis.isEmpty()) {
            return false;
        }

        int serverNumber = GetHashCode(key);
        Long res = _redis.get(serverNumber).del(key);

        return (res == 1);
    }
}
