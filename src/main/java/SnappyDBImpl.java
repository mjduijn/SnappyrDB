import com.esotericsoftware.kryo.Kryo;
import org.iq80.leveldb.*;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.*;

public class SnappyDBImpl implements SnappyDB {
    private DB db;
    private Kryo kryo;
    //TODO close DB on completion

    private SnappyDBImpl(final Context context) throws IOException{
        db = factory.open(new File(context.getPath()), context.getOptions());
        kryo = new Kryo();
    }

    private void close() throws IOException {
        db.close();
    }

    public void dummy() {
        //DUmmy method for testing!
        System.out.println("Dummy!");
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
        try {
            this.db.put(bytes(key), bytes(value));
            return Observable.just((SnappyDB)this);
        }
        catch(DBException e) {
            return Observable.error(e);
        }
    }

    @Override
    public Observable<String> get(String key) {
        try {
            String value = new String(this.db.get(bytes(key)));
            if(value != null) {
                return Observable.just(value);
            }
            else {
                return Observable.error(new KeyNotFoundException(key));
            }
        }
        catch(DBException e) {
            return Observable.error(e);
        }
    }


    @Override
    public Observable<SnappyDB> del(String key) {
        try {
            this.db.delete(bytes(key));
            return Observable.just((SnappyDB)this);
        }
        catch(DBException e) {
            return Observable.error(e);
        }
    }

    @Override
    public boolean exists(String key) {
        try {
            String value = new String(this.db.get(bytes(key)));
            if (value != null) {
                return true;
            } else {
                return false;
            }
        }
        catch(DBException e) {
            return false;
        }
    }

    public Observable<Map.Entry<String, byte[]>> getAllKeyValue(final Func2<String, byte[], Boolean> p2) {
        return Observable.create(new OnSubscribeFromSnappyDb(db))
                .flatMap(new Func1<Map.Entry<byte[], byte[]>, Observable<Map.Entry<String, byte[]>>>() {
                    @Override
                    public Observable<Map.Entry<String, byte[]>> call(Map.Entry<byte[], byte[]> e) {
                        String key = new String(e.getKey());
                        if(p2.call(key, e.getValue())) {
                            return Observable.just((Map.Entry<String, byte[]>) new AbstractMap.SimpleEntry<>(key, e.getValue()));
                        }
                        else {
                            return Observable.empty();
                        }
                    }
                })
                ;
    }

    public Observable<Map.Entry<String, byte[]>> getAllKeyValue(final Func1<String, Boolean> p) {
        return getAllKeyValue(new Func2<String, byte[], Boolean> () {
            @Override
            public Boolean call(String s, byte[] bs) {
                return p.call(s);
            }
        });
    }

    public Observable<Map.Entry<String, byte[]>> getAllKeyValue() {
        return getAllKeyValue(new Func2<String, byte[], Boolean> () {
            @Override
            public Boolean call(String s, byte[] bs) {
                return true;
            }
        });
    }

    @Override
    public Observable<String> getAllKey(final Func1<String, Boolean> p) {

        return getAllKeyValue(new Func1<String, Boolean>() {
                                  @Override
                                  public Boolean call(String s) {
                                      return p.call(s);
                                  }
        })
        .map(new Func1<Map.Entry<String, byte[]>, String>() {
            @Override
            public String call(Map.Entry<String, byte[]> entry) {
                return entry.getKey();
            }
        });
    }

    @Override
    public Observable<String> getAllKey() {
        return getAllKey(
                new Func1<String, Boolean>() {
                    @Override
                    public Boolean call(String s) {
                        return true;
                    }
                });
    }

}
