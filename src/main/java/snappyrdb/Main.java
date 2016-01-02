package snappyrdb;

import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.functions.Func1;
import snappyrdb.operators.DeleteFrom;
import snappyrdb.operators.Get;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static void main(String [] args)
    {
        System.out.println("Running main");

        SnappyrDB snappyrdb = new SnappyrDB(new Context());

        snappyrdb.query().put("Key2", "Value2").execute();

        System.out.println();
        snappyrdb.query().get("Key2", String.class)
        .subscribe(new Observer<String>(){
            @Override
            public void onNext(String s) {
                System.out.println(s);
            }
            @Override
            public void onError(Throwable t) {
                System.out.println("Snappyr has encountered an error!");
                t.printStackTrace();
            }
            @Override
            public void onCompleted() {
                System.out.println("Snappyr has completed!");
            }
        });

        System.out.println();

        snappyrdb.query().get(new Func1<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return true;
            }
        }, String.class)
                .doOnNext(new Action1<String>() {
                    @Override
                    public void call(String s) {
                        System.out.println("New doOnNext value: " + s);
                    }
                })
        .take(1)
        .subscribe(new Observer<String>(){
            @Override
            public void onNext(String s) {
                System.out.println(s);
            }
            @Override
            public void onError(Throwable t) {
                System.out.println("Snappyr has encountered an error!");
                t.printStackTrace();
            }
            @Override
            public void onCompleted() {
                System.out.println("Snappyr has completed!");
            }
        });

        System.out.println();
        //////////// Test delete /////////////
        snappyrdb.query()
        .put("DeletionKey", "DeletionValue")
        .get("DeletionKey", String.class)
        .subscribe(new Action1<String>() {
            @Override
            public void call(String s) {
                System.out.println("Found DeletionKey (good): " + s);
            }
        });

        snappyrdb.query()
        .del("DeletionKey")
        .get("DeletionKey", String.class)
        .subscribe(new Action1<String>() {
            @Override
            public void call(String s) {
                System.out.println("Found DeletionKey (bad): " + s);
            }
        });

        ///////////// Test array insert //////////
        System.out.println();
        Number[] array = {new AtomicInteger(42), new BigDecimal("10E8"), Double.valueOf(Math.PI)};

        snappyrdb.query()
        .put("array", array)
        .get("array", Number[].class)
        .subscribe(new Action1<Number[]>() {
            @Override
            public void call(Number[] numbers) {
                System.out.println("Found Array!");
                for (Number n : numbers) {
                    System.out.println(n);
                }
            }
        });

        snappyrdb.query()
        .del("array")
        .execute();

        System.out.println();


        ///////// Try out get operator ///////////

        snappyrdb.query()
        .put("Key3", "Value3")
        .put("Key4", "Value4")
        .execute();

        snappyrdb.query()
        .query(new Get(new Func1<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return true;
            }
        }))
        .flatMap(new Func1<Observable<Map.Entry<String, byte[]>>, Observable<Map.Entry<String, byte[]>>>() {
            @Override
            public Observable<Map.Entry<String, byte[]>> call(Observable<Map.Entry<String, byte[]>> entryObservable) {
                return entryObservable;
            }
        })
        .take(1)
        .subscribe(new Action1<Map.Entry<String, byte[]>>() {
            @Override
            public void call(Map.Entry<String, byte[]> stringEntry) {
                System.out.println(stringEntry.getKey());
            }
        });

        System.out.println();

        //////////////// Empty database ///////////
        snappyrdb.query()
        .getKey(new Func1<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return true;
            }
        })
        .lift(new DeleteFrom(snappyrdb))
        .subscribe();

        snappyrdb.close();
    }
}