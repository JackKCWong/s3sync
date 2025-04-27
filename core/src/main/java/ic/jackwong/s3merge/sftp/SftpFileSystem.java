package ic.jackwong.s3merge.sftp;

import ic.jackwong.s3merge.FileObject;
import ic.jackwong.s3merge.SinkFileSystem;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.sftp.client.SftpClient;
import org.apache.sshd.sftp.client.SftpClientFactory;
import org.apache.sshd.sftp.client.extensions.openssh.OpenSSHPosixRenameExtension;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

// https://github.com/apache/mina-sshd/blob/master/docs/client-setup.md
// https://github.com/apache/mina-sshd/blob/master/docs/sftp.md
public class SftpFileSystem implements SinkFileSystem {
    private final SftpClient sftpClient;

    public SftpFileSystem(SftpClient sftpClient) throws IOException {
        this.sftpClient = sftpClient;
    }

    @Override
    public FileObject open(String path) throws IOException {
        return this.new SftpFileObject(path);
    }

    // https://issues.apache.org/jira/browse/SSHD-1345
    @Override
    public void rename(String oldPath, String newPath) throws IOException {
        OpenSSHPosixRenameExtension extension = sftpClient.getExtension(OpenSSHPosixRenameExtension.class);
        if (extension == null) {
            throw new IOException("rename not supported");
        }

        extension.posixRename(oldPath, newPath);
    }

    class SftpFileObject implements FileObject {
        private final String path;

        public SftpFileObject(String path) {
            this.path = path;
        }

        @Override
        public String getName() {
            return path;
        }

        @Override
        public String getDirName() {
            return path.substring(0, path.lastIndexOf('/'));
        }

        @Override
        public boolean isDirectory() {
            return false;
        }

        @Override
        public long getSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputStream write() throws IOException {
            FileChannel fileChannel = sftpClient.openRemoteFileChannel(path,
                    SftpClient.OpenMode.Create,
                    SftpClient.OpenMode.Write,
                    SftpClient.OpenMode.Truncate);

            return new BufferedOutputStream(Channels.newOutputStream(fileChannel));
        }

        @Override
        public InputStream read() throws IOException {
            FileChannel fileChannel = sftpClient.openRemoteFileChannel(path,
                    SftpClient.OpenMode.Read);
            return new BufferedInputStream(Channels.newInputStream(fileChannel));
        }

        @Override
        public void close() {
        }
    }
}
