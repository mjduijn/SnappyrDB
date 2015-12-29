import org.iq80.leveldb.DBException;
import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class PutOperator implements Operator<SnappyDB, SnappyDB> {
    String key;
    String value;

    public PutOperator(String key, String value) {
        System.out.println("Creating a new put operator");
        this.key = key;
        this.value = value;
    }

    @Override
    public Subscriber<? super SnappyDB> call(final Subscriber<? super SnappyDB> s) {
        return new Subscriber<SnappyDB>(s) {
            @Override
            public void onCompleted() {
                /* add your own onCompleted behavior here, or just pass the completed notification through: */
                if(!s.isUnsubscribed()) {
                    s.onCompleted();
                }
            }

            @Override
            public void onError(Throwable t) {
        /* add your own onError behavior here, or just pass the error notification through: */
                if(!s.isUnsubscribed()) {
                    s.onError(t);
                }
            }

            @Override
            public void onNext(SnappyDB item) {
        /* this example performs some sort of simple transformation on each incoming item and then passes it along */
                if(!s.isUnsubscribed()) {
                    try {
                        item.put(key, value);
                        s.onNext(item);
                    }
                    catch(DBException e) {
                        s.onError(e);
                    }
                }
            }
        };
    }
}