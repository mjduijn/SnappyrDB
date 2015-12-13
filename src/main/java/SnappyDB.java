import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.functions.Function;

import java.util.AbstractMap;
import java.util.Map;

/**
 * Created by maarten on 5-12-15.
 */
public interface SnappyDB {
    public class KeyNotFoundException extends Throwable {
        String key;

        protected KeyNotFoundException(String key){
            this.key = key;
        }
    }

    public Observable<SnappyDB> put(String key, String value);

    public Observable<String> get(String key);

    public Observable<SnappyDB> del(String key);

    public boolean exists(String key);

//    public Observable<String> findKeys(Func1<String, Boolean> f);


    public Observable<Map.Entry<String, byte[]>> getAllKeyValue(final Func2<String, byte[], Boolean> p2);

    public Observable<Map.Entry<String, byte[]>> getAllKeyValue(final Func1<String, Boolean> p);

    public Observable<Map.Entry<String, byte[]>> getAllKeyValue();

    public Observable<String> getAllKey(final Func1<String, Boolean> p);

    public Observable<String> getAllKey();

    public void dummy();
}