package ic.jackwong.s3sync;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;

public interface FileSystem {
    List<String> list(URI root) throws IOException;

    void copy(URI src, URI dest, Collection<String> files) throws IOException;

    void removeFrom(URI dest, Collection<String> files) throws IOException;
}
