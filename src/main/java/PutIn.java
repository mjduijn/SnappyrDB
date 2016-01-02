import org.iq80.leveldb.DB;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;

import java.util.Map;

public class PutIn implements Operator<SnappyrDB, Map.Entry<String, Object>> {
    SnappyrDB db;

    public PutIn(SnappyrDB db) {
        this.db = db;
    }

    @Override
    public Subscriber<? super Map.Entry<String, Object>> call(final Subscriber<? super SnappyrDB> s) {
        return new Subscriber<Map.Entry<String, Object>>(s) {
            @Override
            public void onCompleted() {
                if (!s.isUnsubscribed()) {
                    s.onNext(db); //Only passes on a single DB item
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
            public void onNext(Map.Entry<String, Object> entries) {
                if (!s.isUnsubscribed()) {
                    db.query()
                    .put(entries.getKey(), entries.getValue())
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