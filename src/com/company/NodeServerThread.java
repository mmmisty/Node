package com.company;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mialiu on 10/11/15.
 */
public class NodeServerThread implements Runnable {
    private TaskQueue<Socket> _taskQueue;
    private static int _threadIdCounter;
    private int _threadId;

    static private Map<String, String> _primaryData = new HashMap<>();
    //private HashMap<String, String> _seocondData = null;

    private Socket _socket = null;

    static private int _totalPutTime = 0;
    static private int _totalGetTime = 0;
    static private int _totalDelTime = 0;

    public NodeServerThread (TaskQueue<Socket> taskQueue) {
        _threadId = _threadIdCounter;
        _threadIdCounter += 1;

        _taskQueue = taskQueue;
    }

    @Override
    public void run() {
        while (true) {
            _socket = _taskQueue.Get();
            if (_socket == null) {
                break;
            }

            System.out.println("start process socket " + _threadId);

            ProcessCommand();

            System.out.println("end process socket " + _threadId);

            try {
                _socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected void ProcessCommand() {
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(_socket.getInputStream()));

            while (true) {

                String OneCommand = input.readLine();
                //System.out.println("NodeServer: " + OneCommand);
                if ( null == OneCommand || OneCommand.isEmpty() ) {
                    System.out.println("Total time: put" + _totalPutTime + ", get " + _totalGetTime + ", del " + _totalDelTime);
                    break;
                }
                String header = OneCommand.substring(0, 4);
                String key = OneCommand.substring(4, 4 + 20);
                String value = OneCommand.substring(4 + 20, 4 + 20 + 1000);

                String result;
                if (header.equals("put ")) {
                    result = Put(key, value);
                } else if (header.equals("get ")){
                    result = Get(key);
                } else if (header.equals("del ")) {
                    result = Del(key);
                } else {
                    System.out.println("Unknow Command: " + OneCommand);
                    result = HandleUnknown();
                }

                BufferedWriter output = new BufferedWriter(new OutputStreamWriter(_socket.getOutputStream()));
                output.write(result);
                output.newLine();
                output.flush();
            }
        } catch (Exception ex) {
            System.out.println("ProcessCommand error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    protected String Put(String key, String value) {
        Date start = new Date();
//        System.out.println("PUT, Key: " + key);
//        System.out.println("Value: " + value);
        synchronized (_primaryData) {
            _primaryData.put(key, value);
            Date end = new Date();
            _totalPutTime += end.getTime() - start.getTime();
        }
        return "Done.";
    }

    protected String Get(String key) {
        Date start = new Date();
        //System.out.println("GET, Key: " + key);
        String value;
        synchronized (_primaryData) {
            value = _primaryData.getOrDefault(key, "");
            Date end = new Date();
            _totalPutTime += end.getTime() - start.getTime();
        }
        return value;
    }

    protected String Del(String key) {
        Date start = new Date();
        //System.out.println("Key: " + key);
        synchronized (_primaryData) {
            _primaryData.remove(key);
            Date end = new Date();
            _totalPutTime += end.getTime() - start.getTime();
        }
        return "Done.";
    }

    protected String HandleUnknown() {
        return "Currently, \"get\", \"put\", and \"get\" are the only supported commands.";
    }
}
