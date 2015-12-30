import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;

import java.io.ByteArrayOutputStream;
import java.util.AbstractMap;
import java.util.Map;

public class SnappyrQuery {

    private Kryo kryo = new Kryo();

    private Observable<DB> db;

    public SnappyrQuery(DB db) {
        if(db != null) {
            this.db = Observable.just(db);
        }
        else {
            this.db = Observable.error(new NullPointerException("No database given to SnappyrQuery"));
        }

        this.kryo = new Kryo();
    }

    protected SnappyrQuery(Observable<DB> prev) {
        this.db = prev;

        this.kryo = new Kryo();
        this.kryo.setAsmEnabled(true);
    }

    public SnappyrQuery query(Observable.Operator<DB, DB> operator) {
        return new SnappyrQuery(db.lift(operator));
    }

    public SnappyrQuery put(String key, Object value) {
        return query(new Put(key, value));
    }

    public void putKryo(final String key, String value) {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        kryo.register(value.getClass());
        Output output = new Output(stream);

        try {
            kryo.writeObject(output, value);
            output.close();

            db.subscribe(new Action1<DB>() {
                             @Override
                             public void call(DB entries) {
                                 entries.put(key.getBytes(), stream.toByteArray());
                             }
                         });



        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Observable<Map.Entry<String, byte[]>> getKeyValue(final Func1<String, Boolean> keyPred, final Func1<byte[], Boolean> valuePred) {
        return db.flatMap(new Func1<DB, Observable<Map.Entry<String, byte[]>>>() {
            @Override
            public Observable<Map.Entry<String, byte[]>> call(DB entries) {
                return Observable.create(new OnSubscribeFromSnappyDb(entries))
                        .flatMap(new Func1<Map.Entry<byte[], byte[]>, Observable<Map.Entry<String, byte[]>>>() {
                            @Override
                            public Observable<Map.Entry<String, byte[]>> call(Map.Entry<byte[], byte[]> e) {
                                String key = new String(e.getKey());
                                if(keyPred.call(key) && valuePred.call(e.getValue())) {
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

    public Observable<String> getKey(final Func1<String, Boolean> keyPred, final Func1<byte[], Boolean> valuePred) {
        return getKeyValue(keyPred, valuePred)
                .map(new Func1<Map.Entry<String, byte[]>, String>() {
                    @Override
                    public String call(Map.Entry<String, byte[]> entry) {
                        return entry.getKey();
                    }
                });
    }
    public Observable<String> getKey(final Func1<String, Boolean> keyPred) {
        return getKey(keyPred, new Func1<byte[], Boolean>() {
            @Override
            public Boolean call(byte[] bs) {
                return true;
            }
        });
    }
    public Observable<String> getKey() {

        return getKey(new Func1<String, Boolean>() {
                    @Override
                    public Boolean call(String s) {
                        return true;
                    }
                });
    }

    public Observable<byte[]> get(final String key) {
        return db.flatMap(new Func1<DB, Observable<byte[]>>() {
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
        return get(key).flatMap(new Func1<byte[], Observable<T>>() {
            @Override
            public Observable<T> call(byte[] bytes) {
                Input input = new Input(bytes);
                Observable<T> o = Observable.error(new KryoException());
                try {
//                    Kryo kryo = new Kryo();
//                    kryo.register(String.class, 0);
//                    String s = kryo.readObject(input, String.class);

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
        return db.subscribe(onNext, onError, onComplete);
    }

    public Subscription execute() {
        return db.subscribe();
    }


    //TODO verify args: keys may not be empty
}
