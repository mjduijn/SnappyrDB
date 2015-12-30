import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.AbstractMap;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class Get implements Operator<Map.Entry<String, byte[]>, DB> {
    Func1<String, Boolean> keyPred;
    Func1<byte[], Boolean> valuePred;

    public Get(final Func1<String, Boolean> keyPred, final Func1<byte[], Boolean> valuePred) {
        System.out.println("Creating a new put operator");
        this.keyPred = keyPred;
        this.valuePred = valuePred;
    }

    @Override
    public Subscriber<? super DB> call(final Subscriber<? super Map.Entry<String, byte[]>> s) {
        return new Subscriber<DB>(s) {
            @Override
            public void onCompleted() {
                if(!s.isUnsubscribed()) {
//                    s.onCompleted();
                    //Only complete after reading is completed
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





//                Observable.create(new OnSubscribeFromSnappyDb(db))
//                    .flatMap(new Func1<Map.Entry<byte[], byte[]>, Observable<Map.Entry<String, byte[]>>>() {
//                        @Override
//                        public Observable<Map.Entry<String, byte[]>> call(Map.Entry<byte[], byte[]> e) {
//                            String key = new String(e.getKey());
//                            if(keyPred.call(key) && valuePred.call(e.getValue())) {
//                                if(!s.isUnsubscribed()) {
//                                    s.onNext((Map.Entry<String, byte[]>) new AbstractMap.SimpleEntry<>(key, e.getValue()));
//                                }
//                            }
//                        }
//                    })




//                    try {
//                        Observable.create(new OnSubscribeFromSnappyDb(db))
//                            .flatMap(new Func1<Map.Entry<byte[], byte[]>, Observable<Map.Entry<String, byte[]>>>() {
//                                @Override
//                                public Observable<Map.Entry<String, byte[]>> call(Map.Entry<byte[], byte[]> e) {
//                                    String key = new String(e.getKey());
//                                    if(keyPred.call(key) && valuePred.call(e.getValue())) {
////                                        s.onNext(Map.Entry<String, byte[]>) new AbstractMap.SimpleEntry<>(key, e.getValue())));
//                                    }
//                                }
//                            })
//                            ;
//                    }
//                    catch(DBException e) {
//                        s.onError(e);
//                    }
//                }
            }
        };
    }
}
