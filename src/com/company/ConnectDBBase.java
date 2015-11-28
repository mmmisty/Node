package com.company;

import java.io.FileInputStream;
import java.net.Socket;
import java.util.*;

/**
 * Created by mialiu on 11/27/15.
 */
public class ConnectDBBase {
    protected List<String> _servers = null;
//    protected List<Socket> _serverSockets = null;

    protected List<String> _keys = null;
    protected List<String> _values = null;

    public boolean GetConfig(String path) {
        try {
            FileInputStream stream = new FileInputStream(path);
            Properties p = new Properties();
            p.load(stream);
            stream.close();

            String str = p.getProperty("number");
            int num = Integer.parseInt(str);
            _servers = new ArrayList<>(num);

            for (int i = 0; i < num; i++) {
                str = p.getProperty("server" + i);
                _servers.add(i, str);
            }

            return true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return false;
    }

    public boolean ConnectAllServers() {
        System.out.println("In super");
        return true;
    }

    public boolean Put(String key, String value) {
        System.out.println("In super");
        return true;
    }

    public boolean Get(String key, String value) {
        System.out.println("In super");
        return true;
    }

    public boolean Del(String key) {
        System.out.println("In super");
        return true;
    }

    public void Close() {
    }

    protected int GetHashCode(String str) {
        int hashcode = str.hashCode();
        return (hashcode & 0xfffffff) % _servers.size();
    }

    public void Generate(int loop) {
        _keys = new ArrayList<String>(loop);
        _values = new ArrayList<String>(loop);

        int keyLength = 10;
        int valueLength = 90;

        UUID uuid;
        String tmp;
        String all = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQGSTUVWXYZ0123456789";
        StringBuffer buffer;
        Random random = new Random();

        for (int i = 0; i< loop; i++) {
            uuid = UUID.randomUUID(); // 067e6162-3b6f-4ae2-a171-2470b63dff00
            tmp = uuid.toString().substring(0, keyLength);
            if (!_keys.contains(tmp)) {
                _keys.add(tmp);
                buffer = new StringBuffer(uuid.toString().substring(keyLength));
                for (int j = 0; j < valueLength + keyLength - uuid.toString().length(); j++) {
                    int ch = random.nextInt(all.length());
                    buffer.append(all.charAt(ch));
                }
                _values.add(buffer.toString());
//                System.out.println(i + "    key: " + _keys.get(i) + " / value: " + _values.get(i));
            }
        }

    }

    public void ProcessOne(String command, int loop) {
        Date start = new Date();
        boolean result = false;

        for (int i = 0; i < loop; i++) {
            if (command.equals("put")) {
                result = Put(_keys.get(i), _values.get(i));
            } else if (command.equals("get")) {
                result = Get(_keys.get(i), _values.get(i));
            } else if (command.equals("del")) {
                result = Del(_keys.get(i));
            }

            if (!result) {
                System.out.println("Failed to " + command + " key:" + _keys.get(i));
            }
            //System.out.println(i);
        }

        Date end = new Date();
        System.out.println(command + "ing " + loop + " key/value pairs needs " + (end.getTime() - start.getTime()) + " ms.");
    }

    public void Run(String[] args) {
        int loop = Integer.parseInt(args[0]);

        GetConfig("DBs.properties");
        ConnectAllServers();

        long round = 20000;
        Date begin = new Date();
        long t = begin.getTime() % round;
        try {
            System.out.println("Count down " + (round - t)/1000 + " s.");
            Thread.sleep(round - t);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("Test case begins.");

        Generate(loop);
        ProcessOne("put", loop);
        ProcessOne("get", loop);
        ProcessOne("del", loop);

        Close();
    }
}
