import org.iq80.leveldb.DB;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;

public class SnappyrQuery {

    private Observable<DB> db;

    public SnappyrQuery(DB db) {
        if(db != null) {
            this.db = Observable.just(db);
        }
        else {
            this.db = Observable.error(new NullPointerException("No database given to SnappyrQuery"));
        }
    }

    protected SnappyrQuery(Observable<DB> prev) {
        this.db = prev;
    }

    public SnappyrQuery query(Observable.Operator<DB, DB> operator) {
        return new SnappyrQuery(db.lift(operator));
    }

    public SnappyrQuery put(String key, String value) {
        return query(new PutOperator(key, value));
    }

    public Subscription execute(final Action1<? super DB> onNext, final Action1<Throwable> onError, final Action0 onComplete) {
        return db.subscribe(onNext, onError, onComplete);
    }

    public Subscription execute() {
        return db.subscribe();
    }
}
