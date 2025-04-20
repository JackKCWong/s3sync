package ic.jackwong.s3sftp;

import java.io.OutputStream;

public class S3Object {
    public String getKey() {
        return null;
    }

    public boolean isDirectory() {
        return false;
    }

    // https://stackoverflow.com/questions/66808064/get-an-s3object-inputstream-from-a-getobjectresponse-in-aws-java-sdk-2-using-s3a
    // https://stackoverflow.com/questions/54447306/get-an-s3object-from-a-getobjectresponse-in-aws-java-sdk-2-0
    public void copyTo(OutputStream os) {

    }
}
