package ic.jackwong.s3merge;

import java.io.IOException;
import java.util.List;

public interface SrcFileSystem {
    List<FileObject> list(String dir) throws IOException;
}
