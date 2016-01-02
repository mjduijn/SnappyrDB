import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.internal.operators.OnSubscribeCombineLatest;
import rx.internal.operators.OperatorWithLatestFrom;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static void main(String [] args)
    {
        System.out.println("Running main");

        Observable<SnappyDB> snappy = SnappyDBImpl.create(new Context());

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



        /*
        First attempt at a putoperator
        snappy
                .lift(new PutOperator("key1", "value1"))
                .subscribe(new Observer<SnappyDB>(){
                    @Override
                    public void onNext(SnappyDB s) {
                        System.out.println(s);
                    }
                    @Override
                    public void onError(Throwable t) {
                        System.out.println("Reactive snappy has encountered an error!");
                        t.printStackTrace();
                    }
                    @Override
                    public void onCompleted() {
                        System.out.println("Reactive snappy is completed!");
                    }
                });
         */

        /*snappy
                .flatMap(new Func1<SnappyDB, Observable<SnappyDB>>() {
                    @Override
                    public Observable<SnappyDB> call(SnappyDB s) {
                        SnappyDB s2 = s;

                        System.out.println("Running puts");

                        s.put("Dummy1", "Dummy1 value")
                                .put("Tester1", "Tester1 first value")
                                .put("Tester2", "Tester2 value")
                                .dummy();
                        return Observable.just(s2);
                    }
                })
//                .doOnNext(new Action1<SnappyDB>() {
//                    @Override
//                    public void call(SnappyDB s) {
//                        s.put("T1", "Tester2Value");
//                        s.dummy();
//                        s.del("T1");
//                        assert (s.exists("T1") == false);
//                    }
//                })
                .flatMap(new Func1<SnappyDB, Observable<String>>() {
                    @Override
                    public Observable<String> call(SnappyDB s) {
                        return s.getAllKey(new Func1<String, Boolean>() {
                            @Override
                            public Boolean call(String s) {
                                return s.equals("Tester1");
                            }
                        });
                    }
                })

//                .flatMap(new Func1<SnappyDB, Observable<Map.Entry<String, byte[]>>>() {
//                    @Override
//                    public Observable<Map.Entry<String, byte[]>> call(SnappyDB s) {
//                        return s.getAllKeyValue(new Func1<String, Boolean>() {
//                            @Override
//                            public Boolean call(String s) {
//                                return s.contains("Tester");
//                            }
//                        });
//                    }
//                })
//                .subscribe(new Observer<Map.Entry<String, byte[]>>(){
//                    @Override
//                    public void onNext(Map.Entry<String, byte[]> e) {
//                        System.out.println(e.getKey() + " = "  + new String(e.getValue()));
//                    }
//                    @Override
//                    public void onError(Throwable t) {
//                        System.out.println("Reactive snappy has encountered an error!");
//                        t.printStackTrace();
//                    }
//                    @Override
//                    public void onCompleted() {
//                        System.out.println("Reactive snappy is completed!");
//                    }
//                });
                .subscribe(new Observer<String>(){
                @Override
                public void onNext(String s) {
                    System.out.println(s);
                }
                @Override
                public void onError(Throwable t) {
                    System.out.println("Reactive snappy has encountered an error!");
                    t.printStackTrace();
                }
                @Override
                public void onCompleted() {
                    System.out.println("Reactive snappy is completed!");
                }
            });
*/
        snappy
        .flatMap(new Func1<SnappyDB, Observable<String>>() {
            @Override
            public Observable<String> call(SnappyDB s) {
                return s.getAllKey();
            }
        })
        .subscribe(new Observer<String>(){
            @Override
            public void onNext(String s) {
                System.out.println(s);
            }
            @Override
            public void onError(Throwable t) {
                System.out.println("2nd Reactive snappy has encountered an error!");
                t.printStackTrace();
            }
            @Override
            public void onCompleted() {
                System.out.println("Reactive snappy has completed!");
            }
        });


        /*
        //Verifying that PutOperator can't be used on arbitrary reactive streams
        Observable.just("Only value")
                .lift(new PutOperator("key", "value"))
                .subscribe();
         */

    }
}

/* Old way of putting

snappy
.flatMap(new Func1<SnappyDB, Observable<SnappyDB>>() {
    @Override
    public Observable<SnappyDB> call(SnappyDB s) {
        return s.put("Dummy1", "Dummy1 value")
    }
})
 */
