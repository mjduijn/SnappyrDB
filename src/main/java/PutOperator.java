import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class PutOperator implements Operator<DB, DB> {
    String key;
    String value;

    public PutOperator(String key, String value) {
        System.out.println("Creating a new put operator");
        this.key = key;
        this.value = value;
    }

    @Override
    public Subscriber<? super DB> call(final Subscriber<? super DB> s) {
        return new Subscriber<DB>(s) {
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
            public void onNext(DB item) {
        /* this example performs some sort of simple transformation on each incoming item and then passes it along */
                if(!s.isUnsubscribed()) {
                    try {
                        item.put(bytes(key), bytes(value));
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