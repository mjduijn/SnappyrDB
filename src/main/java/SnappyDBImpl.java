import com.esotericsoftware.kryo.Kryo;
import org.iq80.leveldb.*;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.*;

public class SnappyDBImpl implements SnappyDB {

    private static HashMap<String, DB> dbs = new HashMap<>();
    private static Kryo kryo = new Kryo();

    private DB db;
    private Observable<DB> db2;
    //TODO close DB on completion
    //TODO factory class?
    //TODO finish chain with exec() call

    private SnappyDBImpl(Context context, String dbName) throws IOException {
        File file = new File(context.getPath(), dbName);

        if(!dbs.containsKey(file.getPath())) {
            DB db = factory.open(file, context.getOptions());
            dbs.put(file.getPath(), db);
        }
        this.db = dbs.get(file.getPath());
        this.db2 = Observable.just(dbs.get(file.getPath()));
    }

    private SnappyDBImpl(Context context) throws IOException {
        this(context, "default_db_name");
    }

    private void close() throws IOException {
        db.close();
    }

    public void dummy() {
        //Dummy method for testing
        System.out.println("Dummy!");
        this.db2
                .subscribe(new Observer<DB>(){
                    @Override
                    public void onNext(DB db) {
                        System.out.println("Subscribing db");
                    }
                    @Override
                    public void onError(Throwable t) {
                        System.out.println("Reactive snappy has encountered an error!");
                        t.printStackTrace();
                    }
                    @Override
                    public void onCompleted() {
                        System.out.println("Reactive snappy is completed!");
                    }
                });
    }

    public static Observable<SnappyDB> create(final Context context) {
        return Observable.create(new Observable.OnSubscribe<SnappyDB>() {
            SnappyDBImpl s;

            @Override
            public void call(final Subscriber<? super SnappyDB> subscriber) {
                if (!subscriber.isUnsubscribed()) {
                    try {
                        s = new SnappyDBImpl(context);
                        subscriber.onNext(s);
                        subscriber.onCompleted();
                    } catch (IOException e) {
                        subscriber.onError(e);
                    }
                }
            }
        });
    }

    @Override
    public void put(String key, String value) throws DBException {
        db.put(bytes(key), bytes(value));
    }

    /*
    //TODO research proper builder pattern
    @Override
    public SnappyDB put(final String key, final String value) {
        this.db2 = this.db2.flatMap(new Func1<DB, Observable<DB>>() {
            @Override
            public Observable<DB> call(DB db) {
                try {
                    db.put(bytes(key), bytes(value));
                    return Observable.just(db);
                }
                catch(DBException e) {
                    return Observable.error(e);
                }
            }
        });
        return this;
    }
    */

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
