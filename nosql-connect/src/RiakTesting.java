import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

/**
 * Created by Tiaotiao on 11/27/2015.
 */
public class RiakTesting {
    public static void Test() throws Exception {
        RiakClient riak = RiakClient.newClient(Settings.HOST);

        String key = "my_key";
        String val = "my_val";

        Namespace ns = new Namespace("default", "my_bucket");
        Location loc = new Location(ns, key);

        // put data
        RiakObject obj = new RiakObject();
        obj.setValue(BinaryValue.create(val));

        StoreValue store = new StoreValue.Builder(obj).withLocation(loc).withOption(StoreValue.Option.W, new Quorum(3)).build();
        riak.execute(store);

        // get data
        FetchValue fetch = new FetchValue.Builder(loc).build();
        FetchValue.Response resp = riak.execute(fetch);

//        System.out.println(resp.toString());

        RiakObject respObj = resp.getValue(RiakObject.class);

        // check
        if (!obj.getValue().equals(respObj.getValue())) {
            System.out.println("Riak test failed: get value not match. " + respObj.getValue());
        }

        // del data
        DeleteValue del = new DeleteValue.Builder(loc).build();
        riak.execute(del);

        System.out.println("Riak Test OK");
    }
}
