import rx.Observable;

/**
 * Created by maarten on 5-12-15.
 */
public interface AbstractSnappyDB {
    public class KeyNotFoundException extends Throwable {
        String key;

        protected KeyNotFoundException(String key){
            this.key = key;
        }
    }

    public Observable<AbstractSnappyDB> put(String key, String value);

    public Observable<String> get(String key);
}
