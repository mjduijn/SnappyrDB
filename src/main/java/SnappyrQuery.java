import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.AbstractMap;
import java.util.Map;

public class SnappyrQuery {

    private Observable<DB> dbObs;

    public SnappyrQuery(DB dbObs) {
        if(dbObs != null) {
            this.dbObs = Observable.just(dbObs);
        }
        else {
            this.dbObs = Observable.error(new NullPointerException("No database given to SnappyrQuery"));
        }
    }

    protected SnappyrQuery(Observable<DB> prev) {
        this.dbObs = prev;
    }

    public SnappyrQuery lift(Observable.Operator<DB, DB> operator) {
        return new SnappyrQuery(dbObs.lift(operator));
    }
    public <T> Observable<T> query(Observable.Operator<T, DB> operator) {
        return dbObs.lift(operator);
    }

    public SnappyrQuery put(String key, Object value) {
        return lift(new Put(key, value));
    }

    public SnappyrQuery del(String key) {
        return lift(new Delete(key));
    }

    //Mother of all get multiple functions
    public Observable<Map.Entry<String, byte[]>> getKeyValue(final Func1<String, Boolean> keyPred) {
        return dbObs.flatMap(new Func1<DB, Observable<Map.Entry<String, byte[]>>>() {
            @Override
            public Observable<Map.Entry<String, byte[]>> call(DB entries) {
                return Observable.create(new OnSubscribeFromSnappyDb(entries))
                        .flatMap(new Func1<Map.Entry<byte[], byte[]>, Observable<Map.Entry<String, byte[]>>>() {
                            @Override
                            public Observable<Map.Entry<String, byte[]>> call(Map.Entry<byte[], byte[]> e) {
                                String key = new String(e.getKey());
//                                System.out.println("Key found: " + key);
                                if(keyPred.call(key)) {
                                    return Observable.just((Map.Entry<String, byte[]>) new AbstractMap.SimpleEntry<>(key, e.getValue()));
                                }
                                else {
                                    return Observable.empty();
                                }
                            }
                        })
                        ;
            }
        });
    }

    public Observable<String> getKey(final Func1<String, Boolean> keyPred) {
        return getKeyValue(keyPred)
                .map(new Func1<Map.Entry<String, byte[]>, String>() {
                    @Override
                    public String call(Map.Entry<String, byte[]> entry) {
                        return entry.getKey();
                    }
                });
    }

    public Observable<byte[]> get(final String key) {
        return dbObs.flatMap(new Func1<DB, Observable<byte[]>>() {
            @Override
            public Observable<byte[]> call(DB entries) {
                try {
                    return Observable.just(entries.get(key.getBytes()));
                }
                catch (DBException e) {
                    return Observable.error(e);
                }
            }
        });
    }

    public <T> Observable<T> get(final String key, final Class<T> className) {
        final Kryo kryo = new Kryo();
        return get(key).flatMap(new Func1<byte[], Observable<T>>() {
            @Override
            public Observable<T> call(byte[] bytes) {
                Input input = new Input(bytes);
                Observable<T> o = Observable.error(new KryoException());
                try {
                    kryo.register(className);
                    o = Observable.just(kryo.readObject(input, className));
                }
                catch (Exception e) {
                    o = Observable.error(e);
                } finally {
                    input.close();
                    return o;
                }
            }
        });
    }

    public Observable<byte[]> get(final Func1<String, Boolean> keyPred) {
        return getKeyValue(keyPred)
                .map(new Func1<Map.Entry<String, byte[]>, byte[]>() {
                    @Override
                    public byte[] call(Map.Entry<String, byte[]> stringEntry) {
                        return stringEntry.getValue();
                    }
                });
    }
    public <T> Observable<T> get(final Func1<String, Boolean> keyPred, final Class<T> className) {
        final Kryo kryo = new Kryo();
        kryo.register(className);

        return get(keyPred).flatMap(new Func1<byte[], Observable<T>>() {
            @Override
            public Observable<T> call(byte[] bytes) {
                Input input = new Input(bytes);
                Observable<T> o = Observable.error(new KryoException());
                try {
                    kryo.register(className);
                    o = Observable.just(kryo.readObject(input, className));
                }
                catch (Exception e) {
                    o = Observable.error(e);
                } finally {
                    input.close();
                    return o;
                }
            }
        });
    }


    public Subscription execute(final Action1<? super DB> onNext, final Action1<Throwable> onError, final Action0 onComplete) {
        return dbObs.subscribe(onNext, onError, onComplete);
    }

    public Subscription execute() {
        return dbObs.subscribe();
    }


    //TODO verify args: keys may not be empty
}
