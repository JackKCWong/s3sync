package ic.jackwong.s3merge;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class S3FileSystem implements SrcFileSystem {
    private final URI root;
    private final String bucket;
    private final S3AsyncClient s3Client;

    public S3FileSystem(S3AsyncClient s3Client, URI root) {
        this.root = root;
        this.bucket = root.getHost();
        this.s3Client = s3Client;
    }

    // https://www.baeldung.com/java-aws-s3-list-bucket-objects#pagination-with-listobjectsv2iterable
    @Override
    public List<FileObject> list(String dir) {
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .maxKeys(10000)
                .build();

        ListObjectsV2Response resp = s3Client.listObjectsV2(listObjectsV2Request).join();

        List<FileObject> objects = resp.contents().stream()
                .map((s3Object) -> this.new S3FileObject(s3Object))
                .sorted(Comparator.comparing(S3FileObject::getName))
                .map((s3FileObject) -> (FileObject) s3FileObject)
                .toList();

        return objects;
    }

    public class S3FileObject implements FileObject {
        private final S3Object s3Object;

        public S3FileObject(S3Object s3Object) {
            this.s3Object = s3Object;
        }

        @Override
        public String getName() {
            return this.s3Object.key().substring(S3FileSystem.this.root.getPath().length() + 1);
        }

        public boolean isDirectory() {
            return this.s3Object.key().endsWith("/");
        }

        @Override
        public long getSize() {
            return this.s3Object.size();
        }

        @Override
        public OutputStream write() throws IOException {
            return null;
        }

        @Override
        public InputStream read() throws IOException {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(S3FileSystem.this.bucket)
                    .key(this.s3Object.key()).build();

            ResponseInputStream<GetObjectResponse> object = S3FileSystem.this.s3Client.getObject(request, AsyncResponseTransformer.toBlockingInputStream()).join();

            return object;
        }

        @Override
        public void close() {

        }

        // https://stackoverflow.com/questions/66808064/get-an-s3object-inputstream-from-a-getobjectresponse-in-aws-java-sdk-2-using-s3a
        // https://stackoverflow.com/questions/54447306/get-an-s3object-from-a-getobjectresponse-in-aws-java-sdk-2-0
        public void copyTo(OutputStream os) {

        }
    }
}
