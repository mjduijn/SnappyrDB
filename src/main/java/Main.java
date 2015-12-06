import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.functions.Func1;

public class Main {
    public static void main(String [] args)
    {
        System.out.println("Running main");

        SnappyDBImpl
        .create(new Context())
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
                .flatMap(new Func1<SnappyDB, Observable<String>>() {
                    @Override
                    public Observable<String> call(SnappyDB s) {
                        return s.get("Tester");
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
