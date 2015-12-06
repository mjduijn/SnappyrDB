import org.iq80.leveldb.*;
import rx.Observable;
import rx.Subscriber;

import java.io.File;
import java.io.IOException;

import static org.fusesource.leveldbjni.JniDBFactory.*;

public class SnappyDBImpl implements SnappyDB {
    private DB db;
    //TODO close DB on completion
    private SnappyDBImpl(final Context context) throws IOException{
        db = factory.open(new File(context.getPath()), context.getOptions());
    }

    private void close() throws IOException {
        db.close();
    }

    public static Observable<SnappyDB> create(final Context context) {
        return Observable.create(new Observable.OnSubscribe<SnappyDB>() {
            SnappyDBImpl s;

            @Override
            public void call(final Subscriber<? super SnappyDB> subscriber) {
                if (!subscriber.isUnsubscribed()) {
                    try {
                        if (!subscriber.isUnsubscribed()) {
                            s = new SnappyDBImpl(context);
                            subscriber.onNext(s);
                        }
                    } catch (IOException e) {
                        if (!subscriber.isUnsubscribed()) {
                            subscriber.onError(e);
                        }
                    }
                }
            }
        });
    }

    @Override
    public Observable<SnappyDB> put(String key, String value) {
        this.db.put(bytes(key), bytes(value));
        return Observable.just((SnappyDB)this);
    }

    @Override
    public Observable<String> get(String key) {
        String value = new String(this.db.get(bytes(key)));
        if(value != null) {
            return Observable.just(value);
        }
        else {
            return Observable.error(new KeyNotFoundException(key));
        }
    }
}

//TODO Streaming??!
