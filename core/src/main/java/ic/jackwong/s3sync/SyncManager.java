package ic.jackwong.s3sync;

import java.io.IOException;
import java.net.URI;
import java.util.*;

public class SyncManager {
    public record SyncResult(Collection<String> newObjects, Collection<String> deletedObjects) {

    }

    public SyncResult sync(URI src, URI dest) throws IOException {
        FileSystem srcFs = FileSystemFactory.create(src);
        return sync(srcFs.list(src), src, dest);
    }

    public SyncResult sync(Collection<String> objects, URI src, URI dest) throws IOException {
        FileSystem srcFs = FileSystemFactory.create(src);
        FileSystem destFs = FileSystemFactory.create(dest);

        List<String> srcObjects = new ArrayList<>(objects);
        List<String> destObjects = destFs.list(dest);

        Set<String> newObjects = new HashSet<>(srcObjects);
        destObjects.forEach(newObjects::remove);
        srcFs.copy(src, dest, newObjects);

        Set<String> deletedObjects = new HashSet<>(destObjects);
        srcObjects.forEach(deletedObjects::remove);
        destFs.removeFrom(dest, deletedObjects);

        return new SyncResult(newObjects, deletedObjects);
    }
}
