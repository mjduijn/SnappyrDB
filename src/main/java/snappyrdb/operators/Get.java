package snappyrdb.operators;

import org.iq80.leveldb.DB;
import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Func1;
import snappyrdb.OnSubscribeFromSnappyDb;

import java.util.AbstractMap;
import java.util.Map;

public class Get implements Operator<Observable<Map.Entry<String, byte[]>>, DB> {
    Func1<String, Boolean> keyPred;

    public Get(final Func1<String, Boolean> keyPred) {
        this.keyPred = keyPred;
    }

    @Override
    public Subscriber<? super DB> call(final Subscriber<? super Observable<Map.Entry<String, byte[]>>> s) {
        return new Subscriber<DB>() {
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
            public void onNext(DB db) {
                s.onNext(
                        Observable.create(new OnSubscribeFromSnappyDb(db))
                        .flatMap(new Func1<Map.Entry<byte[], byte[]>, Observable<Map.Entry<String, byte[]>>>() {
                            //Filter and convert
                            @Override
                            public Observable<Map.Entry<String, byte[]>> call(Map.Entry<byte[], byte[]> e) {
                                String key = new String(e.getKey());
                                if(keyPred.call(key)) {
                                    return Observable.just((Map.Entry<String, byte[]>) new AbstractMap.SimpleEntry<>(key, e.getValue()));
                                }
                                else {
                                    return Observable.empty();
                                }
                            }
                        })
                );
            }
        };
    }
}