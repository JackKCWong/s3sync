package ic.jackwong.s3merge;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class S3MergeManager {

    private final Executor executor;
    private final SrcFileSystem srcFileSystem;
    private final DestFileSystem destFileSystem;

    public S3MergeManager(int parallelism, SrcFileSystem srcFileSystem, DestFileSystem destFileSystem) {
        this.executor = Executors.newWorkStealingPool(parallelism);
        this.srcFileSystem = srcFileSystem;
        this.destFileSystem = destFileSystem;
    }

    public S3MergeManager(Executor executor, SrcFileSystem srcFileSystem, DestFileSystem destFileSystem) {
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
        }, executor);
    }

    public CompletableFuture<BulkTransferResult> mergeSubdirs(String srcDir, String destDir, RenameFunction destNameMapper) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<FileObject> objects = this.srcFileSystem.list(srcDir).stream().filter(FileObject::isDirectory).toList();
                List<CompletableFuture<TransferResult>> transferResults = new ArrayList<>();
                for (FileObject object : objects) {
                    CompletableFuture<TransferResult> transferResult = mergeTo(object.getDirName(), destNameMapper.rename(object.getDirName(), destDir));
                    transferResults.add(transferResult);
                }
                return new BulkTransferResult(transferResults);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    public interface RenameFunction {
        String rename(String originalName, String destDir);
    }
}
