package snappyrdb;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import rx.*;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import snappyrdb.operators.Delete;
import snappyrdb.operators.Get;
import snappyrdb.operators.KryoConvertTo;
import snappyrdb.operators.Put;

import java.util.Map;

public class SnappyrQuery {

    private Observable<DB> dbObs;

    public SnappyrQuery(DB dbObs) {
        if(dbObs != null) {
            this.dbObs = Observable.just(dbObs);
        }
        else {
            this.dbObs = Observable.error(new NullPointerException("No database given to snappyrdb.SnappyrQuery"));
        }
    }

    public SnappyrQuery(Observable<DB> prev) {
        this.dbObs = prev;
    }

    public <T> Observable<T> lift(Observable.Operator<T, DB> operator) {
        return dbObs.lift(operator);
    }

    public SnappyrQuery query(Observable.Operator<DB, DB> operator) {
        return new SnappyrQuery(this.lift(operator));
    }

    public <T> SnappyrQuery put(String key, T value) {
        return query(new Put(key, value));
    }

    public SnappyrQuery del(String key) {
        return query(new Delete(key));
    }

    //Mother of all get multiple functions
    public Observable<Map.Entry<String, byte[]>> getKeyValue(final Func1<String, Boolean> keyPred) {
        return this.lift(new Get(keyPred))
                .flatMap(new Func1<Observable<Map.Entry<String, byte[]>>, Observable<Map.Entry<String, byte[]>>>() {
                    @Override
                    public Observable<Map.Entry<String, byte[]>> call(Observable<Map.Entry<String, byte[]>> entryObservable) {
                        return entryObservable;
                    }
                }); // Flatten
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
                    byte[] bytes = entries.get(key.getBytes());
                    if(bytes != null) {
                        return Observable.just(bytes);
                    }
                    else {
                        System.out.println("Could not find key");
                        return Observable.empty();
                    }
                }
                catch (DBException e) {
                    return Observable.error(e);
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    public <T> Observable<T> get(final String key, final Class<T> className) {
        return get(key)
                .lift(new KryoConvertTo(className));
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

    @SuppressWarnings("unchecked")
    public <T> Observable<T> get(final Func1<String, Boolean> keyPred, final Class<T> className) {
        return get(keyPred)
            .lift(new KryoConvertTo(className));
    }

    public SnappyrQuery subscribeOn(Scheduler scheduler) {
        return new SnappyrQuery(this.dbObs.subscribeOn(scheduler));
    }

    public SnappyrQuery observeOn(Scheduler scheduler) {
        return new SnappyrQuery(this.dbObs.observeOn(scheduler));
    }

    public Subscription subscribe(final Action1<? super DB> onNext, final Action1<Throwable> onError, final Action0 onComplete) {
        return dbObs.subscribe(onNext, onError, onComplete);
    }

    public Subscription subscribe(final Action1<Throwable> onError, final Action0 onComplete) {
        return dbObs.subscribe(new Action1<DB>() {
            @Override
            public void call(DB entries) {
                //do nothing
            }
        }, onError, onComplete);
    }

    public Subscription subscribe(final Action1<? super DB> onNext) {
        return dbObs.subscribe(onNext);
    }

    @SuppressWarnings("unchecked")
    public Subscription subscribe(Observer observer) {
        return dbObs.subscribe(observer);
    }

    @SuppressWarnings("unchecked")
    public Subscription subscribe(Subscriber subscriber) {
        return dbObs.subscribe(subscriber);
    }

    public Subscription subscribe() {
        return dbObs.subscribe();
    }
}
