package ic.jackwong.s3merge;

public interface SinkFileSystem {
    FileObject open(String filepath);
}
