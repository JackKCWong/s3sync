package ic.jackwong.s3merge;


public record TransferResult(long bytesTransferred, FileObject destObject) {
}
