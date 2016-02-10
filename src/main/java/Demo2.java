import snappyrdb.Context;
import snappyrdb.SnappyrDB;

public class Demo2 {

    public static void main(String... args) {
        System.out.println("Running demo2");
        SnappyrDB snappyrdb = new SnappyrDB(new Context());

        snappyrdb.query()
                .get(key -> true, Contributor.class)
                .subscribe((value) -> System.out.println(value),
                        (e) -> e.printStackTrace(),
                        () -> System.out.println("Retrieved all values in database"));
    }
}
