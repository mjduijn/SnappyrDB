/**
 * Created by maarten on 5-12-15.
 */
import rx.functions.Action1;

public class Main {
    public static void main(String [] args)
    {
        System.out.println("Running main");

        SnappyDB
        .create(new Context())
        .subscribe(new Action1<SnappyDB>() {
            @Override
            public void call(SnappyDB snappyDB) {
                snappyDB.print("snappy is printing!");
            }
        });
    }
}
