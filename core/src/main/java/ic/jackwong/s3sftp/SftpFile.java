package ic.jackwong.s3sftp;

import java.io.OutputStream;

public interface SftpFile {
    OutputStream write();

    String name();

    // https://issues.apache.org/jira/browse/SSHD-1345
    boolean rename(String newName);
}
