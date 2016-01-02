package snappyrdb;

import org.iq80.leveldb.Options;

public class Context {
    private String path;
    private Options options;

    public Context(String path) {
        this.path = path;
        this.options = new Options();
        options.createIfMissing(true);
    }

    public Context() {
        this("./");
    }

    public String getPath() {
        return path;
    }

    public Options getOptions() {
        return options;
    }

}
