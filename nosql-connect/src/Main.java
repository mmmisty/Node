import java.io.IOException;

/**
 * Created by Tiaotiao on 11/20/2015.
 */
public class Main {

    public static void main(String[] arg) {

        try {
            RiakTesting.Test();
//            RedisClient.Test();
//            MongoDBClient.Test();
//            HBaseClient.Test();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
