package com.company;

public class Main {

    public static void main(String[] args) {
	// write your code here
        // Node server
//        NodeServer server1 = new NodeServer(args[0]);
//        server1.Start();
//
//        // Node Client
//        Commander c1 = new Commander();
//        //c1.SetConfig();
//        c1.Run();

//        TestCase t = new TestCase();
//        t.Run(args);

        if (args[1].equals("redis")) {
            ConnectRedis r = new ConnectRedis();
            r.Run(args);
        } else if (args[1].equals("mongodb")) {
            ConnectMongoDB m = new ConnectMongoDB();
            m.Run(args);
        } else if (args[1].equals("riak")) {
            ConnectRiak r2 = new ConnectRiak();
            r2.Run(args);
        } else if (args[1].equals("hbase")) {
            ConnectHBase h = new ConnectHBase();
            h.Run(args);
        }
    }
}
