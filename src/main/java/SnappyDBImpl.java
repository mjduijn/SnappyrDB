import com.esotericsoftware.kryo.Kryo;
import com.sun.org.apache.xpath.internal.operations.Bool;
import javafx.util.Pair;
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

    public void dummy() {
        DBIterator iterator = db.iterator();
        try {
            for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                String key = asString(iterator.peekNext().getKey());
                String value = asString(iterator.peekNext().getValue());
                System.out.println(key+" = "+value);
            }
        } finally {
            try{
                iterator.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Observable.from(db)
                .subscribe(new Observer<Map.Entry<byte[], byte[]>>(){
                    @Override
                    public void onNext(Map.Entry<byte[], byte[]> e) {
                        System.out.println(new String(e.getKey()));
                    }
                    @Override
                    public void onError(Throwable t) {
                        System.out.println("Reactive snappy has encountered an error!");
//                        t.printStackTrace();
                    }
                    @Override
                    public void onCompleted() {
                        System.out.println("Reactive snappy dummy is completed!");
                    }
                });
    }

    public Observable<Map.Entry<String, byte[]>> getAllKeyValue(final Func2<String, byte[], Boolean> p2) {
        return Observable.from(db)
                .map(new Func1<Map.Entry<byte[], byte[]>, Map.Entry<String, byte[]>>() {
                    @Override
                    public Map.Entry<String, byte[]> call(Map.Entry<byte[], byte[]> entry) {
                        return new AbstractMap.SimpleEntry<>(new String(entry.getKey()), entry.getValue());
                    }
                })
                .doOnNext(new Action1<Map.Entry<String, byte[]>>() {
                    @Override
                    public void call(Map.Entry<String, byte[]> stringEntry) {
                        System.out.println(stringEntry.getValue());
                    }
                })
//                .flatMap(new Func1<Map.Entry<String, byte[]>, Observable<Map.Entry<String, byte[]>>>() {
//                    @Override
//                    public Observable<Map.Entry<String, byte[]>> call(Map.Entry<String, byte[]> e) {
//                        if(p2.call(e.getKey(), e.getValue())) {
//                            return Observable.just(e);
//                        }
//                        else {
//                            return Observable.empty();
//                        }
//                    }
//                })
                ;
    } //TODO filter first

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
    public Observable<String> getAllKey(Func1<String, Boolean> p) {
        return getAllKeyValue()
                .map(new Func1<Map.Entry<String, byte[]>, String>() {
                    @Override
                    public String call(Map.Entry<String, byte[]> entry) {
                        return entry.getKey();
                    }
                })
//                .filter(p)
                ;
    } //TODO filter before map

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
