package snappyrdb;

import org.iq80.leveldb.DB;
import rx.Observable;

import java.io.File;
import java.io.IOException;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

public class SnappyrDB {

    private DB db;

    public SnappyrDB(Context context, String dbName) {
        File file = new File(context.getPath(), dbName);

        try {
            db = factory.open(file, context.getOptions());
        }
        catch (IOException e) {
            db = null;
        }
        //TODO properly propagate exception
    }

    public SnappyrDB(Context context) {
        this(context, "default_db_name");
    }

    public SnappyrQuery query() {
        return new SnappyrQuery(db);
    }

    public void close() {
        try {
            db.close();
        }
        catch (IOException e) {

        }
    }
}
