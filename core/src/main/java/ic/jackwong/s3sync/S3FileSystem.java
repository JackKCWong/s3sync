package ic.jackwong.s3sync;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileDownload;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static software.amazon.awssdk.transfer.s3.SizeConstant.MB;

public class S3FileSystem implements FileSystem {
    private final S3TransferManager transferManager;
    private final S3AsyncClient s3AsyncClient;

    public S3FileSystem() {
        String endpoint = System.getProperty("s3.endpoint");
        S3CrtAsyncClientBuilder s3CrtAsyncClientBuilder = S3AsyncClient.crtBuilder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .targetThroughputInGbps(10.0)
                .minimumPartSizeInBytes(8 * MB);

        if (endpoint != null) {
            s3CrtAsyncClientBuilder
                    .endpointOverride(URI.create(endpoint))
                    .forcePathStyle(true)
            ;
        }

        s3AsyncClient = s3CrtAsyncClientBuilder.build();
        transferManager = S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();
    }

    @Override
    public List<String> list(URI uri) throws IOException {
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(uri.getHost())
                .prefix(uri.getPath().substring(1))
                .build();

        CompletableFuture<List<String>> future = s3AsyncClient.listObjectsV2(listObjectsV2Request)
                .thenApply(response -> response.contents()
                        .stream()
                        .map(S3Object::key)
                        .map(key -> key.substring(uri.getPath().length() - 1))
                        .toList());

        return future.join();
    }

    @Override
    public void copy(URI src, URI dest, Collection<String> files) throws IOException {
        Path destPath = Paths.get(dest.getPath());
        List<FileDownload> downloads = files.stream()
                .map(f -> transferManager.downloadFile(
                        req -> {
                            Path destFile = destPath.resolve(f);
                            destFile.getParent().toFile().mkdirs();
                            req
                                    .getObjectRequest(b -> b.bucket(src.getHost()).key((src.getPath() + f).substring(1)))
                                    .destination(destFile);
                        }))
                .toList();

        downloads.forEach(d -> d.completionFuture().join());
    }

    @Override
    public void removeFrom(URI dest, Collection<String> files) throws IOException {
        throw new UnsupportedOperationException();
    }
}
