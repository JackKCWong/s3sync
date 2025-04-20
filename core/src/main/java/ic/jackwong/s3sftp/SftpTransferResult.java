package ic.jackwong.s3sftp;

import java.util.Optional;

public class SftpTransferResult {
    private final Exception exception;
    private final SftpFile sftpFile;

    public SftpTransferResult(SftpFile sftpFile, Exception exception) {
        this.exception = exception;
        this.sftpFile = sftpFile;
    }

    public Optional<SftpFile> getSftpFile() {
        return Optional.ofNullable(sftpFile);
    }

    public Optional<Exception> getException() {
        return Optional.ofNullable(exception);
    }
}
