package ic.jackwong.s3sync;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Testcontainers
public class S3FileSystemTest {
    @Container
    private static S3MockContainer s3Mock = new S3MockContainer("latest");
    private static S3AsyncClient s3Client;

    @BeforeAll
    static void setUp() {
        System.setProperty("s3.endpoint", s3Mock.getHttpEndpoint());
        System.setProperty("aws.region", "default");
        System.setProperty("aws.accessKeyId", "nouse");
        System.setProperty("aws.secretAccessKey", "nouse");

        s3Client = S3AsyncClient.crtBuilder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .endpointOverride(URI.create(s3Mock.getHttpEndpoint()))
                .forcePathStyle(true)
                .build();

        CompletableFuture<CreateBucketResponse> bucketResp = s3Client.createBucket(b -> b.bucket("test-bucket"));
        bucketResp.join();
    }

    @Test
    public void testList(@TempDir Path tempDir) throws IOException {
        createDummyObjects(tempDir, List.of(
                "hello/world/test.txt",
                "hello/test.txt"
        ));

        S3FileSystem s3fs = new S3FileSystem();
        List<String> objs = s3fs.list(URI.create("s3://test-bucket/hello/"));
        assertTrue(objs.contains("world/test.txt"));
        assertTrue(objs.contains("test.txt"));
    }

    void createDummyObjects(Path tempDir, List<String> keys) throws IOException {
        Path tmpFile = tempDir.resolve("test.txt");
        tmpFile.toFile().createNewFile();

        List<CompletableFuture<PutObjectResponse>> resp = keys.stream()
                .map(key -> s3Client.putObject(b -> b.bucket("test-bucket").key(key), tmpFile)).toList();

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
                URI.create("s3://test-bucket/hello/"),
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
