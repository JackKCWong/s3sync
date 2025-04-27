package ic.jackwong.s3merge;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface FileObject extends AutoCloseable {
    String getName();
    String getDirName();
    boolean isDirectory();
    long getSize();
    OutputStream write() throws IOException;
    InputStream read() throws IOException;
    void close() throws IOException;
}
