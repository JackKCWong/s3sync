package ic.jackwong.s3merge;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

public class TransferManager {

    private final Executor executor;
    private final SrcFileSystem srcFileSystem;
    private final DestFileSystem destFileSystem;

    public TransferManager(int parallelism, SrcFileSystem srcFileSystem, DestFileSystem destFileSystem) {
        this.executor = Executors.newWorkStealingPool(parallelism);
        this.srcFileSystem = srcFileSystem;
        this.destFileSystem = destFileSystem;
    }

    public TransferManager(Executor executor, SrcFileSystem srcFileSystem, DestFileSystem destFileSystem) {
        this.executor = executor;
        this.srcFileSystem = srcFileSystem;
        this.destFileSystem = destFileSystem;
    }

    public CompletableFuture<TransferResult> mergeTo(String src, String dest) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<FileObject> objects = this.srcFileSystem.list(src);
                FileObject destObject = this.destFileSystem.open(dest);
                try (OutputStream os = destObject.write()) {
                    long bytesTransferred = 0;
                    for (FileObject object : objects) {
                        if (object.isDirectory()) {
                            continue;
                        }
                        try (InputStream is = object.read()) {
                            bytesTransferred += is.transferTo(os);
                        }
                    }
                    return new TransferResult(bytesTransferred, destObject);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public CompletableFuture<BulkTransferResult> mergeRecursively(String srcDir, String destDir, BiFunction<String, String, String> destNameMapper) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<FileObject> objects = this.srcFileSystem.list(srcDir);
                List<CompletableFuture<TransferResult>> transferResults = new ArrayList<>();
                for (FileObject object : objects) {
                    if (object.isDirectory()) {
                        CompletableFuture<TransferResult> transferResult = mergeTo(object.getName(), destNameMapper.apply(destDir, object.getName()));
                        transferResults.add(transferResult);
                    }
                }
                return new BulkTransferResult(transferResults);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
