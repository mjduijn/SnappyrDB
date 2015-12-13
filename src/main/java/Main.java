import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.functions.Func1;

public class Main {
    public static void main(String [] args)
    {
        System.out.println("Running main");

        Observable<SnappyDB> snappy = SnappyDBImpl.create(new Context());

        snappy
                .flatMap(new Func1<SnappyDB, Observable<SnappyDB>>() {
                    @Override
                    public Observable<SnappyDB> call(SnappyDB s) {
                        return s.put("Tester", "TesterValue");
                    }
                })
                .flatMap(new Func1<SnappyDB, Observable<SnappyDB>>() {
                    @Override
                    public Observable<SnappyDB> call(SnappyDB s) {
                        return s.put("Tester2", "Tester2Value");
                    }
                })
                .doOnNext(new Action1<SnappyDB>() {
                    @Override
                    public void call(SnappyDB s) {
                        s.put("T1", "Tester2Value");
                        s.dummy();
//                        s.del("T1");
//                        assert (s.exists("T1") == false);
                    }
                })

//                .flatMap(new Func1<SnappyDB, Observable<String>>() {
//                    @Override
//                    public Observable<String> call(SnappyDB s) {
//                        return s.get("Tester");
//                    }
//                })

                .flatMap(new Func1<SnappyDB, Observable<String>>() {
                    @Override
                    public Observable<String> call(SnappyDB s) {
                        s.put("T1", "Tester2Value");
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
                        System.out.println("Reactive snappy has encountered an error!");
//                        t.printStackTrace();
                    }
                    @Override
                    public void onCompleted() {
                        System.out.println("Reactive snappy is completed!");
                    }
                });


//                .subscribe(new Observer<AbstractSnappyDB>(){
//                    @Override
//                    public void onNext(AbstractSnappyDB snappyDB) {
//                        snappyDB.print("Reactive snappy is printing!");
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

    }
}
