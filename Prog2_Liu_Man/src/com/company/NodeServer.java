package com.company;

import java.io.FileInputStream;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

/**
 * Created by mialiu on 10/11/15.
 */
public class NodeServer implements Runnable {
    private String _config = "server0.properties";

    private int _port = 0;

    public NodeServer() {
        _config = "server0.properties";
    }

    public NodeServer(String name) {
        _config = name;
    }

    public void Start() {
        Thread t1 = new Thread(this);
        t1.start();
    }

    public void run() {
        if (!GetConfig()) {
            System.out.println("Fail to load server config file " + _config + ".");
            return;
        } else {
            System.out.println("Load server config file " + _config + " successfully.");
        }

        try {
//            int port = 40007;//new Random().nextInt(10000) + 40010;
            ServerSocket listener = new ServerSocket();
            listener.bind(new InetSocketAddress(Inet4Address.getByName("0.0.0.0"), _port));
            System.out.println("Start listening: " + listener.getInetAddress());
            System.out.println("Listening port " + listener.getLocalPort());
            while (true) {
                Socket socket = listener.accept();
                // start a thread
                NodeServerThread thread1 = new NodeServerThread(socket);
                Thread thread2 = new Thread(thread1);
                thread2.start();

                //socket.close();
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    public boolean GetConfig() {
        try {
            FileInputStream stream = new FileInputStream(_config);
            Properties p = new Properties();
            p.load(stream);
            stream.close();

            String str = p.getProperty("serverPort");
            _port = Integer.parseInt(str);

            return true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return false;
    }

}
