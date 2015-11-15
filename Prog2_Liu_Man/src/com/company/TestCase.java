package com.company;

import java.util.Date;

/**
 * Created by mialiu on 10/18/15.
 */
public class TestCase {
    private Commander c = new Commander();

    public void Put1000(){
        Date start = new Date();

        c.GetConfig("servers.properties");
        c.ConnectAllServers();

        for (int i = 0; i < 1000; i++) {
            c.Put("key" + i, "value" + i);
            //System.out.println(i);
        }

        Date end = new Date();
        System.out.println("Putting 1000 key/value pairs needs " + (end.getTime() - start.getTime()) + " ms.");
    }

    public void Get1000(){
        Date start = new Date();

        for (int i = 0; i < 1000; i++) {
            c.Get("key" + i);
            //System.out.println(i);
        }

        Date end = new Date();
        System.out.println("Getting 1000 key/value pairs needs " + (end.getTime() - start.getTime()) + " ms.");

    }

    public void Delete1000(){
        Date start = new Date();

        for (int i = 0; i < 1000; i++) {
            c.Del("key" + i);
            //System.out.println(i);
        }

        Date end = new Date();
        System.out.println("Deleting 1000 key/value pairs needs " + (end.getTime() - start.getTime()) + " ms.");

    }
}
