package ic.jackwong.s3merge;

// https://github.com/apache/mina-sshd/blob/master/docs/client-setup.md
// https://github.com/apache/mina-sshd/blob/master/docs/sftp.md
public interface SftpFileSystem {
    SftpFile resolve(String name);
}
