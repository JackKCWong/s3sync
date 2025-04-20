package ic.jackwong.s3sftp;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class SftpTransferManager {

    private final Executor executor;

    public SftpTransferManager(int parallelism) {
       this.executor = Executors.newWorkStealingPool(parallelism);
    }

    public SftpTransferManager(Executor executor) {
        this.executor = executor;
    }

    public CompletableFuture<SftpTransferResult> transfer(List<S3Object> objects, SftpFile sftpFile) {
        return CompletableFuture.supplyAsync(() -> {
            try (OutputStream dest = sftpFile.write()) {
                objects.forEach(s3Object -> {
                    if (s3Object.isDirectory()) {
                        return;
                    }
                    s3Object.copyTo(dest);
                });
                return new SftpTransferResult(sftpFile, null);
            } catch (IOException e) {
                return new SftpTransferResult(null, e);
            }
        }, executor);
    }
}
