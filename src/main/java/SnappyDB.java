import rx.Observable;

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
}
