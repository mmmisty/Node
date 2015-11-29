package com.company;

public class Main {

    public static void main(String[] args) {
        if (args.length <= 0) {
            printHelp();
            return;
        }

        String cmd = args[0];
        if (cmd.equals("server")) {
            // Node server
            NodeServer server1 = new NodeServer(args[0]);
            server1.Start();
            // Node Client
            Commander c1 = new Commander();
            c1.Run();

        } else if (cmd.equals("test")) {
            TestCase t = new TestCase();
            t.Run(args[1]);

        } else if (cmd.equals("redis")) {
            ConnectRedis r = new ConnectRedis();
            r.Run(args[1]);

        } else if (cmd.equals("mongodb")) {
            ConnectMongoDB m = new ConnectMongoDB();
            m.Run(args[1]);

        } else if (cmd.equals("riak")) {
            ConnectRiak r2 = new ConnectRiak();
            r2.Run(args[1]);

        } else if (cmd.equals("hbase")) {
            ConnectHBase h = new ConnectHBase();
            h.Run(args[1]);
        }
    }

    public static void printHelp() {
        System.out.println("Usage: java -jar TestDB.jar server|test|redis|mongodb|riak|hbase [count]");
    }
}
