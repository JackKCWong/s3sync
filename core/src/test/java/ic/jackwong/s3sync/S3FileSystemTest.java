package ic.jackwong.s3sync;

import ic.jackwong.common.TestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class S3FileSystemTest extends TestBase {
    @Test
    public void testList(@TempDir Path tempDir) throws IOException {
        createDummyObjects(tempDir, List.of(
                "hello/world/test.txt",
                "hello/test.txt"
        ));

        S3FileSystem s3fs = new S3FileSystem();
        List<String> objs = s3fs.list(URI.create("s3://%s/hello/".formatted(TEST_BUCKET)));
        assertTrue(objs.contains("world/test.txt"));
        assertTrue(objs.contains("test.txt"));
    }

    void createDummyObjects(Path tempDir, List<String> keys) throws IOException {
        Path tmpFile = tempDir.resolve("test.txt");
        tmpFile.toFile().createNewFile();

        List<CompletableFuture<PutObjectResponse>> resp = keys.stream()
                .map(key -> s3AsyncClient.putObject(b -> b.bucket(TEST_BUCKET).key(key), tmpFile)).toList();

        resp.forEach(CompletableFuture::join);
    }

    @Test
    public void testSync(@TempDir Path tempDir) throws IOException {
        createDummyObjects(tempDir, List.of(
                "hello/world/test.txt",
                "hello/test.txt"
        ));
        tempDir.resolve("dest").toFile().mkdirs();
        tempDir.resolve("dest/file0.txt").toFile().createNewFile();

        SyncManager.SyncResult syncRes = new SyncManager().sync(
                URI.create("s3://%s/hello/".formatted(TEST_BUCKET)),
                URI.create(tempDir.resolve("dest").toAbsolutePath().toString())
        );

        assertEquals(2, syncRes.newObjects().size());
        assertTrue(syncRes.newObjects().contains("world/test.txt"));
        assertTrue(syncRes.newObjects().contains("test.txt"));
        assertTrue(tempDir.resolve("dest/world/test.txt").toFile().exists());
        assertTrue(tempDir.resolve("dest/test.txt").toFile().exists());
        assertEquals(1, syncRes.deletedObjects().size());
        assertFalse(tempDir.resolve("dest/file0.txt").toFile().exists());
    }
}
