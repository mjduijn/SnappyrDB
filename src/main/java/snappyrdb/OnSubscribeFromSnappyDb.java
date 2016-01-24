package snappyrdb;

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
 * Interface for creating an iterable from the SnappyDb jni iterable
 * Inspired by Rx Java OnSubscribeFromIterable
 */
public class OnSubscribeFromSnappyDb implements Observable.OnSubscribe<Map.Entry<byte[], byte[]>> {
    final DB db;

    public OnSubscribeFromSnappyDb(DB db) {
        if(db == null) {
            throw new NullPointerException("iterable must not be null");
        } else {
            this.db = db;
        }
    }

    public void call(Subscriber<? super Map.Entry<byte[], byte[]>> o) {
        DBIterator it = this.db.iterator();
        it.seekToFirst(); //Needs to be called before starting. Reason for implementing a separate Onsubscribe
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

        IterableProducer(Subscriber<? super T> o, Iterator<? extends T> it) {
            this.o = o;
            this.it = it;
        }

        @Override
        public void request(long n) {
            if (get() == Long.MAX_VALUE) {
                // already started with fast-path
                return;
            }
            if (n == Long.MAX_VALUE && compareAndSet(0, Long.MAX_VALUE)) {
                fastpath();
            } else
            if (n > 0 && BackpressureUtils.getAndAddRequest(this, n) == 0L) {
                slowpath(n);
            }

        }

        void slowpath(long n) {
            // backpressure is requested
            final Subscriber<? super T> o = this.o;
            final Iterator<? extends T> it = this.it;

            long r = n;
            while (true) {
                /*
                 * This complicated logic is done to avoid touching the
                 * volatile `requested` value during the loop itself. If
                 * it is touched during the loop the performance is
                 * impacted significantly.
                 */
                long numToEmit = r;
                while (true) {
                    if (o.isUnsubscribed()) {
                        return;
                    } else if (it.hasNext()) {
                        if (--numToEmit >= 0) {
                            o.onNext(it.next());
                        } else
                            break;
                    } else if (!o.isUnsubscribed()) {
                        o.onCompleted();
                        return;
                    } else {
                        // is unsubscribed
                        return;
                    }
                }
                r = addAndGet(-r);
                if (r == 0L) {
                    // we're done emitting the number requested so
                    // return
                    return;
                }

            }
        }

        void fastpath() {
            // fast-path without backpressure
            final Subscriber<? super T> o = this.o;
            final Iterator<? extends T> it = this.it;

            while (true) {
                if (o.isUnsubscribed()) {
                    return;
                } else if (it.hasNext()) {
                    o.onNext(it.next());
                } else if (!o.isUnsubscribed()) {
                    o.onCompleted();
                    return;
                } else {
                    // is unsubscribed
                    return;
                }
            }
        }
    }

}