package ic.jackwong.s3sync;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SyncManager {
    public record SyncResult(Collection<String> newObjects, Collection<String> deletedObjects) {

    }

    public SyncResult sync(URI src, URI dest) throws IOException {
        FileSystem srcFs = FileSystemFactory.create(src);
        FileSystem destFs = FileSystemFactory.create(dest);
        List<String> srcObjects = srcFs.list(src);
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
