package ic.jackwong.common;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public abstract class S3TestBase {
    @RegisterExtension
    protected static final S3MockExtension s3Mock =
            S3MockExtension.builder().silent().withSecureConnection(false).build();

    protected static S3AsyncClient s3AsyncClient;
    protected static S3Client s3Client;

    @BeforeAll
    static void setUp() {
        System.setProperty("s3.endpoint", s3Mock.getServiceEndpoint());
        System.setProperty("aws.region", "default");
        System.setProperty("aws.accessKeyId", "nouse");
        System.setProperty("aws.secretAccessKey", "nouse");

        s3AsyncClient = S3AsyncClient.crtBuilder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .endpointOverride(URI.create(s3Mock.getServiceEndpoint()))
                .forcePathStyle(true)
                .build();

        s3Client = S3Client.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .endpointOverride(URI.create(s3Mock.getServiceEndpoint()))
                .forcePathStyle(true)
                .build();

    }

    protected final String TEST_BUCKET = UUID.randomUUID().toString();

    @BeforeEach
    void beforeEach() {
        CompletableFuture<CreateBucketResponse> bucketResp = s3AsyncClient.createBucket(b -> b.bucket(TEST_BUCKET));
        bucketResp.join();
    }

    protected PutObjectResponse uploadObject(String key, byte[] content) {
        // Split the key into parts to create directory objects
        String[] parts = key.split("/");
        StringBuilder currentPath = new StringBuilder();
        for (int i = 0; i < parts.length - 1; i++) {
            if (i > 0) {
                currentPath.append("/");
            }
            currentPath.append(parts[i]);
            String dirKey = currentPath + "/";
            // Create an empty object for the directory
            s3AsyncClient.putObject(b -> b.bucket(TEST_BUCKET).key(dirKey), AsyncRequestBody.empty()).join();
        }

        return s3AsyncClient.putObject(b -> b.bucket(TEST_BUCKET).key(key), AsyncRequestBody.fromBytes(content)).join();
    }
}
