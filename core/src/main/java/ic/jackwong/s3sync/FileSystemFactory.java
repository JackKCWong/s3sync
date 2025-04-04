package ic.jackwong.s3sync;

import java.net.URI;
import java.util.List;

public class FileSystemFactory {
    public static FileSystem create(URI root) {
        if (root.getScheme() != null && List.of("s3", "s3a", "s3n").contains(root.getScheme())) {
            return new S3FileSystem();
        } else {
            return new LocalFileSystem();
        }
    }
}
