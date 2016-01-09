package snappyrdb.operators;

import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Func0;
import rx.functions.Func1;

import java.util.AbstractMap;
import java.util.Map;

public class AssignKey <T> implements Operator<Map.Entry<String, T>, T> {
    Func1<T, String> keyGen;

    public AssignKey(final Func1<T, String> keyGen) {
        this.keyGen = keyGen;
    }
    public AssignKey(final Func0<String> keyGen) {
        this.keyGen = new Func1<T, String>() {
            @Override
            public String call(T t) {
                return keyGen.call();
            }
        };
    }
    public AssignKey(final String s) {
        this.keyGen = new Func1<T, String>() {
            @Override
            public String call(T t) {
                return s;
            }
        };
    }

    @Override
    public Subscriber<? super T> call(final Subscriber<? super Map.Entry<String, T>> s) {
        return new Subscriber<T>() {
            @Override
            public void onCompleted() {
                if (!s.isUnsubscribed()) {
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
            public void onNext(T t) {
                try {
                    s.onNext(new AbstractMap.SimpleEntry<>(keyGen.call(t), t));
                }
                catch(Exception e) {
                    s.onError(e);
                }
            }
        };
    }
}