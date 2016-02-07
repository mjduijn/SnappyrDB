package snappyrdb.extenders;

import org.iq80.leveldb.DB;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.ReplaySubject;
import snappyrdb.SnappyrDB;
import snappyrdb.SnappyrQuery;

import java.util.Map;

public class SinglePut<T> implements Func1<Observable.OnSubscribe<T>, SnappyrQuery> {
    SnappyrDB db;
    String key;

    public SinglePut(SnappyrDB db, String key) {
        this.db = db;
        this.key = key;
    }

    @Override
    public SnappyrQuery call(final Observable.OnSubscribe<T> entryOnSubscribe) {

        final ReplaySubject<DB> subj = ReplaySubject.create();
        final SnappyrQuery query = new SnappyrQuery(subj);

        entryOnSubscribe.call(new Subscriber<T>() {
            final Subscriber<T> subscriber = this;

            @Override
            public void onCompleted() {
                subj.onNext(db.getDb());
                subj.onCompleted();
            }

            @Override
            public void onError(Throwable throwable) {
                subj.onError(throwable);
                subscriber.unsubscribe();
            }

            @Override
            public void onNext(T value) {
                 query.put(key, value)
                .subscribe(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable e) {
                        subj.onError(e);
                        subscriber.unsubscribe();
                    }
                }, new Action0() {
                    @Override
                    public void call() {
                        //Do nothing on completed of single lift
                    }
                });
            }
        });
        return query;
    }
}