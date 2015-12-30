import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.Map;

public class Main {

    public static void main(String [] args)
    {
        System.out.println("Running main");

        Observable<SnappyDB> snappy = SnappyDBImpl.create(new Context());

        SnappyrDB snappyrdb = new SnappyrDB(new Context());

        snappyrdb.query().put("Key2", "Value2").execute();
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
                System.out.println("Reactive snappy is completed!");
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
