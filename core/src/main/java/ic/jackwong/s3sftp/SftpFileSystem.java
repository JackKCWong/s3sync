package ic.jackwong.s3sftp;

// https://github.com/apache/mina-sshd/blob/master/docs/sftp.md
public interface SftpFileSystem {
    SftpFile resolve(String name);
}
