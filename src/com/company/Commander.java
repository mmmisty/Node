package com.company;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by mialiu on 10/11/15.
 */
public class Commander {
    private List<String> _servers = null;
    private List<Socket> _serverSockets = null;

    private FileWriter _outputFile = null;

    public void Run() {
        String assertString = "";
        try {
            _outputFile = new FileWriter("output.txt", true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        String config = "servers.properties";
        if (!GetConfig(config)) {
            assertString = "Having trouble to load " + config + "...\n";
            PrintStringLine(assertString);
            return;
        } else {
            assertString = "Load " + config + " successfully.";
//            System.out.println("Load " + config + " successfully.");
            PrintStringLine(assertString);
        }

//        ConnectAllServers();
        System.out.println("Make sure all servers are active.");

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        while (true) {
            System.out.println("Please input your choices: 1.put 2.get 3. del");
            Scanner s1 = new Scanner(System.in);
            String ans = s1.nextLine();
            if (ans.isEmpty()) {
                break;
            }
//            String ans = "1";
            int choice = Integer.parseInt(ans);

            ConnectAllServers();

            switch (choice) {
                case 1:
                    System.out.println("Please input the key");
                    String key = s1.nextLine();
//                    String key = "key1";

                    System.out.println("Please input the value");
                    String value = s1.nextLine();
//                    String value = "value1";


                    if (Put(key, value)) {
                        PrintStringLine("Put successfully with key " + key + " and value " + value);
                    } else {
                        PrintStringLine("Put failed. Unfortunately, the key " + key + " & value " + value + " is not stored.");
                    }
                    break;
                case 2:
                    System.out.println("Please input your key");
                    key = s1.nextLine();
//                    key = "key1";

                    value = Get(key);

                    if (!value.equals("")) {
                        PrintStringLine("Get: for the key " + key + ", the value is " + value);
                    } else {
                        PrintStringLine("Get: for the key " + key + ", find nothing.");
                    }
                    break;
                case 3:
                    System.out.println("Please input your key");
                    key = s1.nextLine();
//                    key = "key1";

                    if (Del(key)) {
                        PrintStringLine("Delete successfully with the key " + key);
                    } else {
                        PrintStringLine("Delete: Unfortunately, the key " + key + " is not deleted.");
                    }
                    break;
                default:
                    PrintStringLine("Unknown command: Let's try again.");
            }
        }
        try {
            _outputFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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
//                _servers.add(str);
                _servers.add(i, str);
            }

            return true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return false;
    }

    public boolean ConnectAllServers() {
        if (_serverSockets == null || _serverSockets.isEmpty()) {
            try {
                _serverSockets = new ArrayList<>(_servers.size());
                for (int i = 0; i < _servers.size(); i++) {
                    String[] args = _servers.get(i).split(":");
                    String host = args[0];
                    int port = Integer.parseInt(args[1]);

                    Socket s1 = new Socket(host, port);
                    _serverSockets.add(i, s1);
                }
                return true;

            } catch (Exception e) {
//                e.printStackTrace();
                _serverSockets.clear();
                System.out.println("Connect failed:" + e.getMessage());
                return false;
            }
        }
        return true;
    }

    protected int GetHashCode(String str) {
        int hashcode = str.hashCode();
        return (hashcode & 0xfffffff) % _servers.size();
    }

    public boolean Put(String key, String value) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_serverSockets == null || _serverSockets.isEmpty()) {
//            ConnectAllServers();
            return false;
        }

        int serverNumber = GetHashCode(key);

        boolean resultPrimary = PutKV2Server(key, value, _serverSockets.get(serverNumber));
        boolean resultSecondary = PutKV2Server(key, value,
                _serverSockets.get((serverNumber + 1) % _servers.size()));

        return resultPrimary || resultSecondary;
    }

    public String Get(String key) {
        if (_servers == null || _servers.isEmpty()) {
            return "";
        }
        if (_serverSockets == null || _serverSockets.isEmpty()) {
            return "";
        }
        int serverNumber = GetHashCode(key);
        String result = GetVFromServer(key, _serverSockets.get(serverNumber));
        if (result.equals("")) {
            result = GetVFromServer(key, _serverSockets.get((serverNumber + 1) % _servers.size()));
        }
        return result;
    }

    public boolean Del(String key) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_serverSockets == null || _serverSockets.isEmpty()) {
            return false;
        }
        int serverNumber = GetHashCode(key);
        boolean resultPramary = DelKVFromServer(key, _serverSockets.get(serverNumber));
        boolean resultSecondary = DelKVFromServer(key, _serverSockets.get((serverNumber + 1) % _servers.size()));
        return resultPramary || resultSecondary;
    }

    public void SetConfig() {
        try {
            FileOutputStream stream = new FileOutputStream("servers.properties");
            Properties p = new Properties();
            //p.load(stream);

            p.setProperty("number", "8");
            //String str = p.getProperty("number");
            //int num = Integer.parseInt(str);
            //_servers = new ArrayList<>(num);

            for (int i = 0; i < 8; i++) {
                //str = p.getProperty("server" + i);
                //_servers.add(str);
                p.setProperty("server" + i, "127.0.0.1:" + String.valueOf(40000 + i));
            }
            p.store(stream, "servers");

            stream.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public boolean PutKV2Server(String key, String value, Socket socket) {
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            // Messages is fixed size at 1024 bytes
            // Header is the first 4 bytes
            // Key is next 20 bytes
            // value is last 1000 bytes
            String message = String.format("%-4s%-20s%-1000s", "put", key, value);
            out.println(message);
            out.flush();

            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line = input.readLine();

            return line.equals("Done.");
        } catch (Exception e) {
            System.out.println("Put Error: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    public String GetVFromServer(String key, Socket socket) {
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            // Messages is fixed size at 1024 bytes
            // Header is the first 4 bytes
            // Key is next 20 bytes
            // value is last 1000 bytes
            String message = String.format("%-4s%-20s%-1000s", "get", key, "");
            out.println(message);
            out.flush();

            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line = input.readLine();

            return line;
        } catch (IOException IOEx) {
            IOEx.printStackTrace();
        }
        return "";
    }

    public boolean DelKVFromServer(String key, Socket socket) {
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            // Messages is fixed size at 1024 bytes
            // Header is the first 4 bytes
            // Key is next 20 bytes
            // value is last 1000 bytes
            String message = String.format("%-4s%-20s%-1000s", "del", key, "");
            out.println(message);
            out.flush();

            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line = input.readLine();

            return line.equals("Done.");
        } catch (IOException IOEx) {
            IOEx.printStackTrace();
        }
        return false;
    }

    protected void PrintStringLine(String str) {
        System.out.println(str);
        try {
            _outputFile.write(str + "\n");
            _outputFile.flush();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                _outputFile.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

}
