package com.company;

public class Main {

    public static void main(String[] args) {
	// write your code here
        // Node server
        NodeServer server1 = new NodeServer(args[0]);
        server1.Start();

        // Node Client
        Commander c1 = new Commander();
        //c1.SetConfig();
        c1.Run();

/*        TestCase t = new TestCase();
        t.Put1000();
        t.Get1000();
        t.Delete1000();*/
    }
}
