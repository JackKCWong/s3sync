package ic.jackwong.s3merge;

import java.io.*;
import java.nio.file.Path;

public class LocalFileSystem implements SinkFileSystem {
    private final Path root;

    public LocalFileSystem(Path root) {
        this.root = root;
    }

    @Override
    public FileObject open(String dest) {
        return new LocalFileObject(new File(root.toAbsolutePath().toString(), dest));
    }
}

class LocalFileObject implements FileObject {
    private final File file;

    LocalFileObject(File file) {
        this.file = file;
    }

    @Override
    public String getName() {
        return file.getName();
    }

    @Override
    public String getDirName() {
        return this.file.getParent();
    }

    @Override
    public boolean isDirectory() {
        return file.isDirectory();
    }

    @Override
    public long getSize() {
        return -1;
    }

    @Override
    public OutputStream write() throws IOException {
        try {
            return new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public InputStream read() throws IOException {
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {

    }
}