package ic.jackwong.common;

import org.apache.sshd.client.ClientBuilder;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.AsyncAuthException;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.auth.password.PasswordChangeRequiredException;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;

import java.util.List;

public class SftpUtils {
    public static SshServer startSshServer(int port) throws Exception {
        SshServer sshd = SshServer.setUpDefaultServer();
        sshd.setPort(port);
        sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
        sshd.setSubsystemFactories(List.of(new SftpSubsystemFactory()));
//        sshd.setCommandFactory(new ScpCommandFactory());
//        sshd.setShellFactory(new EchoShellFactory());
        sshd.setPasswordAuthenticator((username, password, session) -> true);
        sshd.start();

        return sshd;
    }

    public static SshClient startClient() {
        SshClient client = new ClientBuilder()
                .serverKeyVerifier((clientSession, remoteAddress, serverKey) -> true)
                .build(true);

        client.start();

        return client;
    }
}
