package ic.jackwong.s3merge;

public interface DestFileSystem {
    FileObject open(String dest);
}
