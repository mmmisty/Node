package com.company;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mialiu on 11/27/15.
 */
public class ConnectHBase extends ConnectDBBase {
    private List<Table> _hbase = null;
    private List<Connection> _connection = null;

//    public static Configuration conf;
//    public static Admin admin;
//    public static Connection conn;

    public static final String table_name = "myTable";
    public static final String col_family_name = "myColumnFamily";
    public static final String row_name = "myRow";

    public boolean ConnectAllServers() {
        if (_hbase == null || _hbase.isEmpty()) {
            try {
                _hbase = new ArrayList<>(_servers.size());
                _connection = new ArrayList<>(_servers.size());
                for (int i = 0; i < _servers.size(); i++) {
                    String host = _servers.get(i);
                    // connect
                    Configuration conf = HBaseConfiguration.create();
                    conf.set("hbase.zookeeper.quorum", host);

                    Connection conn = ConnectionFactory.createConnection(conf);
                    Admin admin = conn.getAdmin();
                    // create table
                    HTableDescriptor table = new HTableDescriptor(TableName.valueOf(table_name));

                    table.addFamily(new HColumnDescriptor(col_family_name));

                    if (!admin.tableExists(table.getTableName())) {
//                        admin.disableTable(table.getTableName());
//                        admin.deleteTable(table.getTableName());
                        admin.createTable(table);
                    }


                    Table t1 = conn.getTable(TableName.valueOf(table_name));

                    _hbase.add(i, t1);
                    _connection.add(i, conn);
                }
                return true;

            } catch (Exception e) {
//                e.printStackTrace();
                _hbase.clear();
                _connection.clear();
                System.out.println("HBase connect failed. " + e.getMessage());
                return false;
            }
        }
        return true;
    }

    public boolean Put(String key, String value) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_hbase == null || _hbase.isEmpty()) {
            return false;
        }

        int serverNumber = GetHashCode(key);
        Table table = _hbase.get(serverNumber);

        Put p = new Put(Bytes.toBytes(row_name));
        p.addColumn(Bytes.toBytes(col_family_name), Bytes.toBytes(key), Bytes.toBytes(value));
        try {
            table.put(p);
        } catch (IOException e) {
            System.out.println("In Exception: " + e.getMessage());
            return false;
        }

        return true;
    }

    public boolean Get(String key, String value) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_hbase == null || _hbase.isEmpty()) {
            return false;
        }

        int serverNumber = GetHashCode(key);
        Table table = _hbase.get(serverNumber);

        Get g = new Get(Bytes.toBytes(row_name));
        try {
            Result r = table.get(g);
            byte[] bValue = r.getValue(Bytes.toBytes(col_family_name),
                    Bytes.toBytes(key));
            //System.out.println(key + " " + value + "  " + Bytes.toString(bValue));
            return value.equals( Bytes.toString(bValue) );
        } catch (Exception e) {
            System.out.println("In Exception: " + e.getMessage());
            return false;
        }
    }

    public boolean Del(String key) {
        if (_servers == null || _servers.isEmpty()) {
            return false;
        }
        if (_hbase == null || _hbase.isEmpty()) {
            return false;
        }

        int serverNumber = GetHashCode(key);
        Table table = _hbase.get(serverNumber);

        Delete d = new Delete(Bytes.toBytes(row_name));
        try {
            table.delete(d);
        } catch (IOException e) {
            System.out.println("In Exception: " + e.getMessage());
            return false;
        }
        return true;
    }

    public void Close() {
        try {
            for (int i = 0; i < _hbase.size(); i++) {
                Table t1 = _hbase.get(i);
                t1.close();
                _connection.get(i).close();
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
