import redis.clients.jedis.Jedis;

/**
 * Created by Tiaotiao on 11/20/2015.
 */

public class RedisClient {

    public static void Test() {
        boolean ok = true;
        Jedis redis = new Jedis(Settings.HOST);

        String key = "key";
        String val = "value123123";

        redis.configSet("stop-writes-on-bgsave-error", "no");

        redis.set(key, val);

        String getval = redis.get(key);

        if (!getval.equals(val)) {
            System.out.println("get value not match " + getval);
            ok = false;
        }

        Long n = redis.del(key);
        if (n != 1) {
            System.out.println("del value failed "+ n);
            ok = false;
        }

        if (ok) {
            System.out.println("Redis test OK");
        } else {
            System.out.println("Redis test FAILED");
        }
    }

}
