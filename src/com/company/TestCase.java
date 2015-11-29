package com.company;

import java.util.*;

/**
 * Created by mialiu on 10/18/15.
 */
public class TestCase {
    private Commander c = new Commander();
    private List<String> _keys = null;
    private List<String> _values = null;

    public void Run(String arg) {
        int loop = Integer.parseInt(arg);

        Init();

        long round = 20000;
        Date begin = new Date();
        long t = begin.getTime() % round;
        try {
            System.out.println("Count down. " + (round - t) + " ms.");
            Thread.sleep(round - t);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("Test case begins.");

        Generate(loop);
        Put1000(loop);
        Get1000(loop);
        Delete1000(loop);
    }

    public void Init() {
        c.GetConfig("servers.properties");
        c.ConnectAllServers();
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

    public void Put1000(int loop){
        Date start = new Date();
        Boolean result = false;

        for (int i = 0; i < loop; i++) {
//            c.Put("key" + i, "value" + i);
            result = c.Put(_keys.get(i), _values.get(i));
            if (!result) {
                System.out.println("Failed to put key:" + _keys.get(i) + ", value:" + _values.get(i));
            }
            //System.out.println(i);
        }

        Date end = new Date();
        System.out.println("Putting " + loop + " key/value pairs needs " + (end.getTime() - start.getTime()) + " ms.");
    }

    public void Get1000(int loop){
        Date start = new Date();
        String result;

        for (int i = 0; i < loop; i++) {
//            c.Get("key" + i);
            result = c.Get(_keys.get(i));
            if (result == null || result.isEmpty()) {
                System.out.println("Failed to get key:" + _keys.get(i));
            }
            //System.out.println(i);
        }

        Date end = new Date();
        System.out.println("Getting " + loop + " key/value pairs needs " + (end.getTime() - start.getTime()) + " ms.");

    }

    public void Delete1000(int loop){
        Date start = new Date();
        boolean result;

        for (int i = 0; i < loop; i++) {
//            c.Del("key" + i);
            result = c.Del(_keys.get(i));
            if (!result) {
                System.out.println("Failed to delete key:" + _keys.get(i));
            }
            //System.out.println(i);
        }

        Date end = new Date();
        System.out.println("Deleting " + loop + " key/value pairs needs " + (end.getTime() - start.getTime()) + " ms.");

    }
}
