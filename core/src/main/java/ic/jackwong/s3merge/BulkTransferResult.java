package ic.jackwong.s3merge;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public record BulkTransferResult(List<CompletableFuture<TransferResult>> transfers) {
}
