import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;

import java.io.ByteArrayOutputStream;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class Put implements Operator<DB, DB> {
    String key;
    Object value;
    Kryo kryo;

    public Put(String key, Object value) {
        this.key = key;
        this.value = value;
        this.kryo = new Kryo();
        kryo.register(value.getClass());
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
                        ByteArrayOutputStream stream = new ByteArrayOutputStream();
                        Output output = new Output(stream);
                        kryo.writeObject(output, value);
                        output.close();
                        item.put(bytes(key), stream.toByteArray());
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