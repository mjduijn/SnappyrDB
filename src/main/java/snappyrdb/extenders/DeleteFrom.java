package snappyrdb.extenders;

import org.iq80.leveldb.DB;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import snappyrdb.SnappyrDB;
import rx.Observable;
import rx.functions.Func1;
import rx.subjects.ReplaySubject;
import snappyrdb.SnappyrQuery;

public class DeleteFrom implements Func1<Observable.OnSubscribe<String>, SnappyrQuery> {
    SnappyrDB db;

    public DeleteFrom(SnappyrDB db) {
        this.db = db;
    }

    @Override
    public SnappyrQuery call(final Observable.OnSubscribe<String> entryOnSubscribe) {

        final ReplaySubject<DB> subj = ReplaySubject.create();
        final SnappyrQuery query = new SnappyrQuery(subj);

        entryOnSubscribe.call(new Subscriber<String>() {
            final Subscriber<String> subscriber = this;

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
            public void onNext(String s) {
                query.del(s)
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