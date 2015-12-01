import org.iq80.leveldb.*;
import static org.fusesource.leveldbjni.JniDBFactory.*;
import java.io.*;

public class Main {
    public static void main(String [ ] args)
    {
        System.out.println("Running main");

        DB db;

        try {
            // Use the db in here....
            Options options = new Options();
            options.createIfMissing(true);
            db = factory.open(new File("example"), options);

            db.put(bytes("Tampa"), bytes("rocks"));
            String value = asString(db.get(bytes("Tampa")));
            System.out.println(value);
            db.delete(bytes("Tampa"));

            db.close();
        }
        catch (Exception e) {
            System.out.println("Caught IO Exception");
        }
        finally {
        }


    }
}
