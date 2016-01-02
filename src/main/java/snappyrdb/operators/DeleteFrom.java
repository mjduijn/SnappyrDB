package snappyrdb.operators;

import org.iq80.leveldb.DB;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import snappyrdb.SnappyrDB;

public class DeleteFrom implements Operator<SnappyrDB, String> {
    SnappyrDB db;

    public DeleteFrom(SnappyrDB db) {
        this.db = db;
    }

    @Override
    public Subscriber<? super String> call(final Subscriber<? super SnappyrDB> s) {
        return new Subscriber<String>(s) {
            @Override
            public void onCompleted() {
                if (!s.isUnsubscribed()) {
                    s.onNext(db);
                    s.onCompleted();
                }
            }

            @Override
            public void onError(Throwable t) {
                if (!s.isUnsubscribed()) {
                    s.onError(t);
                }
            }

            @Override
            public void onNext(String entries) {
                if (!s.isUnsubscribed()) {
                    db.query().
                    del(entries)
                    .execute(new Action1<DB>() {
                                 @Override
                                 public void call(DB entries) {
                                     //Do nothing
                                 }
                             },
                            new Action1<Throwable>() {
                                @Override
                                public void call(Throwable throwable) {
                                    s.onError(throwable);
                                }
                            },
                            new Action0() {
                                @Override
                                public void call() {
                                    //Do nothing
                                }
                            });
                }
            }
        };
    }
}