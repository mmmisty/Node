import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Tiaotiao on 11/21/2015.
 */
public class HBaseClient {

    public static Configuration conf;
    public static Admin admin;
    public static Connection conn;

    public static final String table_name = "myTable";
    public static final String col_family_name = "myColumnFamily";
    public static final String row_name = "myRow";

    public static void Test() throws IOException {
        connect(Settings.HOST);

        createTable(table_name);

        System.out.println("get table");
        Table table = conn.getTable(TableName.valueOf(table_name));

        System.out.println("Put");
        // Put
        Put p = new Put(Bytes.toBytes(row_name));
        p.addColumn(Bytes.toBytes(col_family_name), Bytes.toBytes("someQualifier"), Bytes.toBytes("Some Value"));
        table.put(p);

        // Get
        Get g = new Get(Bytes.toBytes(row_name));
        Result r = table.get(g);
        byte [] value = r.getValue(Bytes.toBytes(col_family_name),
                Bytes.toBytes("someQualifier"));

        String valueStr = Bytes.toString(value);
        System.out.println("GET: " + valueStr);

        // Delete

        Delete d = new Delete(Bytes.toBytes(row_name));
        table.delete(d);


        table.close();
        conn.close();

        System.out.println("HBase test OK");
    }

    public static void connect(String address) throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", address);

        System.out.println("Connecting...");
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    public static void createTable(String tableName) throws IOException {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

        table.addFamily(new HColumnDescriptor(col_family_name));


        System.out.println("checking table ...");
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }

        System.out.println("creating table ...");
        admin.createTable(table);
    }
}
