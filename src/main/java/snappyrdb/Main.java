package snappyrdb;

import rx.Observable;
import rx.Observer;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import snappyrdb.operators.AssignKey;
import snappyrdb.extenders.DeleteFrom;
import snappyrdb.operators.Get;
import snappyrdb.extenders.PutIn;

import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static void main(String [] args)
    {
        System.out.println("Running main");

        SnappyrDB snappyrdb = new SnappyrDB(new Context());

        snappyrdb.query().put("Key2", "Value2").subscribe();

        ////////////////// PutIn //////////////
        System.out.println();
        snappyrdb.query()
        .get("Key2", String.class)
        .map(new Func1<String, Map.Entry<String, String>>() {
            @Override
            public Map.Entry<String, String> call(String s) {
                return new AbstractMap.SimpleEntry<>("Key2", "ValueReplaced");
            }
        })
        .extend(new PutIn<String>(snappyrdb))
        .get("Key2", String.class)
        .subscribe(new Observer<String>(){
            @Override
            public void onNext(String s) {
                System.out.println("Found Key2");
                System.out.println(s);
            }
            @Override
            public void onError(Throwable t) {
                System.out.println("Snappyr has encountered an error!");
                t.printStackTrace();
            }
            @Override
            public void onCompleted() {
                System.out.println("Weird snappyr has completed!");
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
        .put("DeletionKey", "DeletionValue")
        .get("DeletionKey", String.class)
        .subscribe((s) -> System.out.println(s), (e) -> e.printStackTrace(), () -> System.out.println("Demo query executed"));

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
        .subscribe();

        System.out.println();


        ///////// Try out get operator ///////////

        snappyrdb.query()
        .put("Key3", "Value3")
        .put("Key4", "Value4")
        .subscribe();

        snappyrdb.query()
        .lift(new Get(new Func1<String, Boolean>() {
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

        //////////////// Change threads ///////////

        System.out.println("Thread # " + Thread.currentThread().getId() + " is running main");

        snappyrdb.query()
        .get("Key3", String.class)
        .doOnNext(new Action1<String>() {
            @Override
            public void call(String s) {
                System.out.println("Thread # " + Thread.currentThread().getId() + " is before observeon");
            }
        })
        .subscribeOn(Schedulers.newThread())
        .doOnNext(new Action1<String>() {
            @Override
            public void call(String s) {
                System.out.println("Thread # " + Thread.currentThread().getId() + " is after observeon");
            }
        })
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

        ////////////////// Get value and insert under new key ////////////////

        //Insert dummy key value
        snappyrdb.query()
        .put("Key4", "Value4")
        .subscribe();

        snappyrdb.query()
        .get("Key4", String.class)
        .lift(new AssignKey<String>("Key5"))
        .extend(new PutIn<String>(snappyrdb))
        .subscribe();

        //Retrieve dummy key value for validation purposes
        snappyrdb.query()
        .get("Key5", String.class)
        .subscribe(new Action1<String>() {
            @Override
            public void call(String s) {
                System.out.println("Key5 = " + s);
            }
        }, new Action1<Throwable>() {
            @Override
            public void call(Throwable throwable) {

            }
        }, new Action0() {
            @Override
            public void call() {
                System.out.println("Delete validation completed");
            }
        });

        //////////////// Empty database ///////////
        System.out.println();
        snappyrdb.query()
        .getKey(new Func1<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return true;
            }
        })
        .extend(new DeleteFrom(snappyrdb))
        .get(new Func1<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return true;
            }
        }, String.class)
        .subscribe(new Observer<String>(){
            @Override
            public void onNext(String s) {
                System.out.println("Found Key (Bad)");
                System.out.println(s);
            }
            @Override
            public void onError(Throwable t) {
                System.out.println("Snappyr has encountered an error!");
                t.printStackTrace();
            }
            @Override
            public void onCompleted() {
                System.out.println("Delete all snappyr query has completed!");
            }
        });
        snappyrdb.close();
    }
}