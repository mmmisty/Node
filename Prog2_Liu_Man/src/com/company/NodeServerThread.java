package com.company;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mialiu on 10/11/15.
 */
public class NodeServerThread implements Runnable {

    private Map<String, String> _primaryData = new HashMap<>();
    //private HashMap<String, String> _seocondData = null;

    private Socket _socket = null;

    public NodeServerThread (Socket socket) {
        _socket = socket;
    }

    @Override
    public void run() {
        System.out.println(_socket.getRemoteSocketAddress().toString());
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(_socket.getInputStream()));

            while (true) {

                String OneCommand = input.readLine();
                System.out.println("NodeServer: " + OneCommand);
                if ( null == OneCommand || OneCommand.isEmpty() ) {
                    break;
                }
//                int blank = OneCommand.indexOf(' ');
//                String c1 = OneCommand.substring(0, blank);
//                String c2 = OneCommand.substring(blank + 1);
//                String[] args = OneCommand.split(" ");
                String header = OneCommand.substring(0, 4);
                String key = OneCommand.substring(4, 4 + 20);
                String value = OneCommand.substring(4 + 20, 4 + 20 + 1000);

                if (header.equals("put ")) {
                    Put(key, value);
                } else if (header.equals("get ")){
                    Get(key);
                } else if (header.equals("del ")) {
                    Del(key);
                } else {
                    HandleUnknown();
                }

            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        } finally {
            //Unregister();
        }

        // Echo
        //BufferedWriter output = new BufferedWriter(new OutputStreamWriter(_socket.getOutputStream()));
        //output.write("World!" + port);
        //output.flush();

        //_socket.close();
    }

    protected boolean Put(String key, String value) {
        System.out.println("Key: " + key);
        System.out.println("Value: " + value);
        _primaryData.put(key, value);
        return true;
    }

    protected String Get(String key) {
        System.out.println("Key: " + key);
        String value =_primaryData.getOrDefault(key, "");

        try {
            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(_socket.getOutputStream()));
            output.write(value);
            output.newLine();
            output.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

    protected boolean Del(String key) {
        System.out.println("Key: " + key);
        _primaryData.remove(key);
        return true;
    }

    protected void HandleUnknown() {
        System.out.println("Currently, \"get\", \"put\", and \"get\" are the only supported commands.");
    }
}
