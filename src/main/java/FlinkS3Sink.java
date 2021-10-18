import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.io.IOException;

public class FlinkS3Sink extends Base  {


    public static void main(String[] args) throws IOException {

        // Comment these lines if you run this demo inside emr ec2 instances.
        /*
        ProfileCredentialsProvider profileCredentialsProvider = new ProfileCredentialsProvider();
        AWSCredentials credentials = profileCredentialsProvider.getCredentials();
        System.out.println(credentials.getAWSAccessKeyId());
        System.out.println(credentials.getAWSSecretKey());
        */

        String input_path = "";
        String output_path = "";

        DataStreamSource stringDataStreamSource = env.readTextFile(input_path);
        stringDataStreamSource.writeAsText(output_path);


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
