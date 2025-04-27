package ic.jackwong.s3merge;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionException;

public class S3FileSystem implements SourceFileSystem {
    public static final String DELIMITER = "/";
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
    public List<FileObject> list(String dir) throws IOException {
        try {
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(this.root.getPath() + dir)
                    .delimiter(DELIMITER)
                    .build();

            ListObjectsV2Publisher publisher = s3Client.listObjectsV2Paginator(listObjectsV2Request);
            List<FileObject> objects = new ArrayList<>();
            publisher.subscribe(listObjectsV2Response -> {
                listObjectsV2Response.contents().forEach(s3Object -> {
                    // skip the directory itself
                    if (!s3Object.key().endsWith(dir)) {
                        objects.add(new S3FileObject(s3Object));
                    }
                });
                listObjectsV2Response.commonPrefixes().forEach(prefix -> {
                    objects.add(new S3DirObject(prefix.prefix()));
                });

            }).join();

            objects.sort(Comparator.comparing(FileObject::getName));

            return objects;

        } catch (CompletionException e) {
            throw new IOException(e.getCause());
        }
    }

    class S3FileObject implements FileObject {
        private final S3Object s3Object;

        S3FileObject(S3Object s3Object) {
            this.s3Object = s3Object;
        }

        @Override
        public String getName() {
            return this.s3Object.key().substring(this.s3Object.key().lastIndexOf(DELIMITER) + 1);
        }

        @Override
        public String getDirName() {
            return this.s3Object.key().substring(S3FileSystem.this.root.getPath().length(), this.s3Object.key().lastIndexOf(DELIMITER) + 1);
        }

        public boolean isDirectory() {
            return this.s3Object.key().endsWith(DELIMITER) && this.s3Object.size() == 0;
        }

        @Override
        public long getSize() {
            return this.s3Object.size();
        }

        @Override
        public OutputStream write() throws IOException {
            return null;
        }

        // https://stackoverflow.com/questions/66808064/get-an-s3object-inputstream-from-a-getobjectresponse-in-aws-java-sdk-2-using-s3a
        // https://stackoverflow.com/questions/54447306/get-an-s3object-from-a-getobjectresponse-in-aws-java-sdk-2-0
        @Override
        public InputStream read() throws IOException {
            try {
                GetObjectRequest request = GetObjectRequest.builder()
                        .bucket(S3FileSystem.this.bucket)
                        .key(this.s3Object.key()).build();

                ResponseInputStream<GetObjectResponse> object = S3FileSystem.this.s3Client.getObject(request, AsyncResponseTransformer.toBlockingInputStream()).join();

                return object;
            } catch (CompletionException e) {
                throw new IOException(e.getCause());
            }
        }

        @Override
        public void close() {

        }
    }


    class S3DirObject implements FileObject {
        private final String key;

        S3DirObject(String key) {
            this.key = key;
        }

        @Override
        public String getName() {
            return this.key.substring(S3FileSystem.this.root.getPath().length());
        }

        @Override
        public String getDirName() {
            return this.key.substring(S3FileSystem.this.root.getPath().length(), this.key.lastIndexOf(DELIMITER, this.key.length() - 2) + 1);
        }

        public boolean isDirectory() {
            return true;
        }

        @Override
        public long getSize() {
            return 0;
        }

        @Override
        public OutputStream write() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream read() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {

        }
    }
}
