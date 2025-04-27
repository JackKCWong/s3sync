package ic.jackwong.s3merge;

import java.io.IOException;

public interface SinkFileSystem {
    FileObject open(String filepath) throws IOException;

    default void rename(String oldPath, String newPath) throws IOException {
        throw new UnsupportedOperationException();
    }
}
