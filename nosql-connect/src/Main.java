import java.io.IOException;

/**
 * Created by Tiaotiao on 11/20/2015.
 */
public class Main {

    public static void main(String[] arg) {
        RedisClient.Test();
        MongoDBClient.Test();
        try {
            HBaseClient.Test();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
