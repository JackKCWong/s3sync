package ic.jackwong.s3merge;

import ic.jackwong.common.S3TestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.assertj.core.api.Fail.fail;

class TransferManagerTest extends S3TestBase {
    @Test
    public void can_merge_s3_dir_to_a_single_file(@TempDir Path rootDir) throws IOException {
        // given
        uploadObject("/to_merge/testdir/header.gz", mkgz("header\n"));
        uploadObject("/to_merge/testdir/part1.gz", mkgz("test line1\n"));
        uploadObject("/to_merge/testdir/part2.gz", mkgz("test line2\n"));

        // when
        TransferManager transferManager = new TransferManager(1,
                new S3FileSystem(s3AsyncClient, URI.create("s3://%s/to_merge/".formatted(TEST_BUCKET))),
                new LocalFileSystem(rootDir));
        CompletableFuture<TransferResult> transferResult = transferManager.mergeTo(
                "testdir/", "merged.gz"
        );

        transferResult.join();

        // then
        assertThat(transferResult)
                .isCompletedWithValueMatching(
                        (result) -> {
                            FileObject fileObject = result.destObject();
                            assertThat(fileObject.getName()).isEqualTo("merged.gz");

                            try (InputStream is = result.destObject().read()) {
                                byte[] content = new GZIPInputStream(is).readAllBytes();
                                assertThat(new String(content)).isEqualTo("header\ntest line1\ntest line2\n");
                            } catch (IOException e) {
                                fail("Failed to read merged file", e);
                            }

                            return true;
                        });
    }

    @Test
    public void can_merge_s3_dirs_to_multiple_files(@TempDir Path rootDir) throws IOException {
        // given
        uploadObject("/to_merge/testdir/summary.gz", mkgz("summary\n"));
        uploadObject("/to_merge/testdir/key=dir1/part1.gz", mkgz("hello\n"));
        uploadObject("/to_merge/testdir/key=dir1/part2.gz", mkgz("world\n"));
        uploadObject("/to_merge/testdir/key=dir2/part1.gz", mkgz("test2 line1\n"));
        uploadObject("/to_merge/testdir/key=dir2/part2.gz", mkgz("test2 line2\n"));

        rootDir.resolve("merged").toFile().mkdirs();

        // when
        TransferManager transferManager = new TransferManager(2,
                new S3FileSystem(s3AsyncClient, URI.create("s3://%s/to_merge/".formatted(TEST_BUCKET))),
                new LocalFileSystem(rootDir));

        BulkTransferResult result = transferManager.mergeSubdirs("testdir/", "merged/",
                ((originalName, destDir) ->
                        destDir + "/" + originalName.replace("key=", "")
                                .replaceAll("/$", "")
                                .replaceAll("/", "_")
                                .concat(".gz"))).join();

        assertThat(result.transfers()).hasSize(2);

        result.transfers().forEach(r -> r.join());
        List<Path> merged = Files.list(rootDir.resolve("merged/")).toList();
        assertThat(merged).hasSize(2);
        assertThat(new GZIPInputStream(Files.newInputStream(
                rootDir.resolve("merged/testdir_dir1.gz")))
                .readAllBytes()).isEqualTo("hello\nworld\n".getBytes());
        assertThat(new GZIPInputStream(Files.newInputStream(
                rootDir.resolve("merged/testdir_dir2.gz")))
                .readAllBytes()).isEqualTo("test2 line1\ntest2 line2\n".getBytes());
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