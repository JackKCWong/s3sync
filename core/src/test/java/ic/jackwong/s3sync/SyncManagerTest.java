package ic.jackwong.s3sync;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SyncManagerTest {
    @Test
    public void testSync(@TempDir Path rootDir) throws IOException {
        SyncManager syncManager = new SyncManager();
        Path srcDir = rootDir.resolve("src");
        srcDir.toFile().mkdirs();
        srcDir.resolve("file1.txt").toFile().createNewFile();
        srcDir.resolve("test/").toFile().mkdirs();
        srcDir.resolve("test/file2.txt").toFile().createNewFile();

        Path destDir = rootDir.resolve("dest");
        destDir.toFile().mkdirs();
        destDir.resolve("file0.txt").toFile().createNewFile();

        SyncManager.SyncResult result = syncManager.sync(
                srcDir.toUri(),
                destDir.toUri()
        );

        assertTrue(result.deletedObjects().contains("file0.txt"));
        assertTrue(result.newObjects().contains("file1.txt"));
        assertTrue(result.newObjects().contains("test/file2.txt"));
        assertFalse(destDir.resolve("file0.txt").toFile().exists());
        assertTrue(destDir.resolve("file1.txt").toFile().exists());
        assertTrue(destDir.resolve("test/file2.txt").toFile().exists());
    }
}
