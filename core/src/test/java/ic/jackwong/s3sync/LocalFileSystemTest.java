package ic.jackwong.s3sync;

import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.provider.sftp.BytesIdentityInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LocalFileSystemTest {
    @Test
    public void testList(@TempDir Path rootDir) throws IOException {
        LocalFileSystem localfs = new LocalFileSystem();
        // Create test files in temporary directory
        Path subDir = rootDir.resolve("test");
        subDir.toFile().mkdirs();
        Path file1 = subDir.resolve("file1.txt");
        Path file2 = subDir.resolve("file2.txt");
        Files.createFile(file1);
        Files.createFile(file2);

        // Test listing files
        List<String> result = localfs.list(rootDir.toUri());
        assertFalse(result.isEmpty());
        assertTrue(result.contains("test/file1.txt"));
        assertTrue(result.contains("test/file2.txt"));
    }
}
