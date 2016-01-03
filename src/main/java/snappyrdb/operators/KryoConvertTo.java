package snappyrdb.operators;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import rx.Observable.Operator;
import rx.Subscriber;

public class KryoConvertTo<T> implements Operator<T, byte[]> {
    Kryo kryo = new Kryo();
    Class<T> className;

    public KryoConvertTo(Class<T> className) {
        kryo.register(className);
        this.className = className;
    }

    @Override
    public Subscriber<? super byte[]> call(final Subscriber<? super T> s) {
        return new Subscriber<byte[]>() {
            @Override
            public void onCompleted() {
                if(!s.isUnsubscribed()){
                    s.onCompleted();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if(!s.isUnsubscribed()) {
                    s.onError(throwable);
                }
            }

            @Override
            public void onNext(byte[] bytes) {
                if(!s.isUnsubscribed()) {
                    Input input = new Input(bytes);
                    try {
                        s.onNext(kryo.readObject(input, className));
                    }
                    catch (Exception e) {
                        s.onError(e);
                    }
                    finally {
                        input.close();
                    }
                }
            }
        };
    }
}