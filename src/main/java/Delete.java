import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;

import java.io.ByteArrayOutputStream;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class Delete implements Operator<DB, DB> {
    String key;

    public Delete(String key) {
        this.key = key;
    }

    @Override
    public Subscriber<? super DB> call(final Subscriber<? super DB> s) {
        return new Subscriber<DB>(s) {
            @Override
            public void onCompleted() {
                if(!s.isUnsubscribed()) {
                    s.onCompleted();
                }
            }

            @Override
            public void onError(Throwable t) {
                if(!s.isUnsubscribed()) {
                    s.onError(t);
                }
            }

            @Override
            public void onNext(DB item) {
                if(!s.isUnsubscribed()) {
                    try {
                        item.delete(bytes(key));
                        s.onNext(item);
                    }
                    catch(Exception e) {
                        s.onError(e);
                    }
                }
            }
        };
    }
}