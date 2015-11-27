package com.company;

/**
 * Created by mialiu on 11/27/15.
 */
public class ConnectMongoDB extends ConnectDBBase {

    public boolean ConnectAllServers() {
        return true;
    }

    public boolean Put(String key, String value) {
        return true;
    }

    public boolean Get(String key, String value) {
        return true;
    }

    public boolean Del(String key) {
        return true;
    }
}
