package com.company;

import java.io.FileInputStream;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * Created by mialiu on 10/11/15.
 */
public class NodeServer implements Runnable {
    private String _config = "server0.properties";

    private int _workersCount = 32;
    private ServerSocket _listener = null;
    private TaskQueue<Socket> _taskQueue = new TaskQueue<Socket>(8);
    private List<Thread> _workers = new ArrayList<Thread>();
    private int _port = 0;

    public NodeServer() {
        _config = "server0.properties";
    }

    public NodeServer(String name) {
        _config = name;
    }

    public void Start() {
        startWorkers();

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
            _listener = new ServerSocket();
            _listener.bind(new InetSocketAddress(Inet4Address.getByName("0.0.0.0"), _port));
            System.out.println("Start listening: " + _listener.getInetAddress());
            System.out.println("Listening port " + _listener.getLocalPort());
            while (true) {
                Socket socket = _listener.accept();
                boolean ok = _taskQueue.Put(socket);
                if (!ok) {
                    socket.close();
                    System.out.println("Discard socket.");
                }
                // start a thread
//                NodeServerThread thread1 = new NodeServerThread(socket);
//                Thread thread2 = new Thread(thread1);
//                thread2.start();

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

    private void startWorkers() {
        for (int i = 0; i < _workersCount; i++) {
            NodeServerThread st = new NodeServerThread(_taskQueue);
            Thread th = new Thread(st);
            th.start();

            _workers.add(th);
        }
    }

    public void Stop() {
        try {
            _listener.close();

            _taskQueue.Close();
        } catch (Exception e) {
            //System.out.println("Error: " + e.getMessage());
        }

    }

}
