package ic.jackwong.s3sync;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class LocalFileSystem implements FileSystem {
    @Override
    public List<String> list(URI root) throws IOException {
        Path rootDir = Paths.get(root.getPath());
        try (Stream<Path> paths = Files.walk(rootDir)) {
            return paths.filter(Files::isRegularFile).map(p -> rootDir.relativize(p).toString()).toList();
        }
    }

    @Override
    public void copy(URI src, URI dest, Collection<String> files) throws IOException {
        for (String file : files) {
            Path s = Paths.get(src.getPath()).resolve(file);
            Path d = Paths.get(dest.resolve(file));
            d.getParent().toFile().mkdirs();
            Files.copy(s, d);
        }
    }

    @Override
    public void removeFrom(URI dest, Collection<String> files) throws IOException {
        Path destPath = Paths.get(dest.toString());
        for (String file : files) {
            Files.delete(destPath.resolve(file).toAbsolutePath());
        }
    }
}
