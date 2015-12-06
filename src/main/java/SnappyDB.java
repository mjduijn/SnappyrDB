import org.iq80.leveldb.*;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.subscriptions.Subscriptions;

import java.io.File;
import java.io.IOException;

import static org.fusesource.leveldbjni.JniDBFactory.*;

public class SnappyDB implements AbstractSnappyDB {
    private DB db;
    //TODO close DB on completion
    private SnappyDB (final Context context) throws IOException{
        db = factory.open(new File(context.getPath()), context.getOptions());
    }

    private void close() throws IOException {
        db.close();
    }

    public static Observable<AbstractSnappyDB> create(final Context context) {
        return Observable.create(new Observable.OnSubscribe<AbstractSnappyDB>() {
            SnappyDB s;

            @Override
            public void call(final Subscriber<? super AbstractSnappyDB> subscriber) {
                if (!subscriber.isUnsubscribed()) {
                    try {
                        if (!subscriber.isUnsubscribed()) {
                            s = new SnappyDB(context);
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
    public Observable<AbstractSnappyDB> put(String key, String value) {
        this.db.put(bytes(key), bytes(value));
        return Observable.just((AbstractSnappyDB)this);
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
