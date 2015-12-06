import org.iq80.leveldb.*;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.subscriptions.Subscriptions;

import java.io.File;
import java.io.IOException;

import static org.fusesource.leveldbjni.JniDBFactory.*;

public class SnappyDB {
    private DB db;
    //TODO close DB on completion
    private SnappyDB (final Context context) throws IOException{
        db = factory.open(new File(context.getPath()), context.getOptions());
    }

    private void close() throws IOException {
        db.close();
    }

    public static Observable<SnappyDB> create(final Context context) {
        return Observable.create(new Observable.OnSubscribe<SnappyDB>() {
            SnappyDB s;

            @Override
            public void call(final Subscriber<? super SnappyDB> subscriber) {
                if (!subscriber.isUnsubscribed()) {
                    try {
                        if (!subscriber.isUnsubscribed()) {
                            s = new SnappyDB(context);
                            subscriber.onNext(s);
                        }
                    } catch (IOException e) {
                        subscriber.onError(e);
                    }
                }
            }
        });
    }

    public void print(String str) {
        System.out.println(str);
    }
}
