package ic.jackwong.s3merge;

import ic.jackwong.common.S3TestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class TransferManagerTest extends S3TestBase {
    @Test
    public void can_merge_s3_dir_to_a_single_file(@TempDir Path rootDir) throws IOException {
        uploadObject("/to_merge/testdir/header.gz", mkgz("header\n"));
        uploadObject("/to_merge/testdir/part1.gz", mkgz("test line1\n"));
        uploadObject("/to_merge/testdir/part2.gz", mkgz("test line2\n"));

        TransferManager transferManager = new TransferManager(1,
                new S3FileSystem(s3AsyncClient, URI.create("s3://%s/to_merge".formatted(TEST_BUCKET))),
                new LocalFileSystem(rootDir));
        CompletableFuture<TransferResult> transferResult = transferManager.mergeTo(
                "/testdir", "merged.gz"
        );

        transferResult.join();

        assertThat(transferResult).isCompletedWithValueMatching(
                        (result) -> result.destObject().getName().equals("merged.gz"))
                .isCompletedWithValueMatching(
                        (result) -> {
                            try (InputStream is = result.destObject().read();) {
                                byte[] content = new GZIPInputStream(is).readAllBytes();
                                return new String(content).equals("header\ntest line1\ntest line2\n");
                            } catch (IOException e) {
                                e.printStackTrace();
                                return false;
                            }
                        });
    }

    private byte[] mkgz(String content) throws IOException {
        try (
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                GZIPOutputStream gzipOS = new GZIPOutputStream(bytes);
        ) {
            gzipOS.write(content.getBytes());
            gzipOS.finish();
            return bytes.toByteArray();
        }
    }
}