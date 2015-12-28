import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import rx.Observable;
import rx.Producer;
import rx.Subscriber;
import rx.internal.operators.BackpressureUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Interface for creating an interable from the SnappyDb jni iterable
 * Inspired by rx OnSubscribeFromIterable
 */
public class OnSubscribeFromSnappyDb implements Observable.OnSubscribe<Map.Entry<byte[], byte[]>> {
    final DB db;

    //Iterable<Map.Entry<byte[], byte[]>>
    public OnSubscribeFromSnappyDb(DB db) {
        if(db == null) {
            throw new NullPointerException("iterable must not be null");
        } else {
            this.db = db;
        }
    }

    public void call(Subscriber<? super Map.Entry<byte[], byte[]>> o) {
        DBIterator it = this.db.iterator();
        it.seekToFirst();
        if(!it.hasNext() && !o.isUnsubscribed()) {
            o.onCompleted();
        } else {
            o.setProducer(new OnSubscribeFromSnappyDb.IterableProducer(o, it));
        }

    }

    private static final class IterableProducer<T> extends AtomicLong implements Producer {
        private static final long serialVersionUID = -8730475647105475802L;
        private final Subscriber<? super T> o;
        private final Iterator<? extends T> it;

        private IterableProducer(Subscriber<? super T> o, Iterator<? extends T> it) {
            this.o = o;
            this.it = it;
        }

        public void request(long n) {
            if(this.get() != 9223372036854775807L) {
                if(n == 9223372036854775807L && this.compareAndSet(0L, 9223372036854775807L)) {
                    this.fastpath();
                } else if(n > 0L && BackpressureUtils.getAndAddRequest(this, n) == 0L) {
                    this.slowpath(n);
                }

            }
        }

        void slowpath(long n) {
            Subscriber o = this.o;
            Iterator it = this.it;
            long r = n;

            label28:
            while(true) {
                long numToEmit = r;

                while(!o.isUnsubscribed()) {
                    if(!it.hasNext()) {
                        if(!o.isUnsubscribed()) {
                            o.onCompleted();
                            return;
                        }

                        return;
                    }

                    if(--numToEmit < 0L) {
                        r = this.addAndGet(-r);
                        if(r == 0L) {
                            return;
                        }
                        continue label28;
                    }

                    o.onNext(it.next());
                }

                return;
            }
        }

        void fastpath() {
            Subscriber o = this.o;
            Iterator it = this.it;

            while(!o.isUnsubscribed()) {
                if(!it.hasNext()) {
                    if(!o.isUnsubscribed()) {
                        o.onCompleted();
                        return;
                    }

                    return;
                }

                o.onNext(it.next());
            }

        }
    }
}
