package uk.ac.cranfield.cloudcomputing.assignment.common.credentials;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;


public class AWSCredentialsBean
{
    
    private static String accessKeyId = "AKIAJ2KOCJHIWA4JVTYQ";
    private static String secretAccessKey = "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";
    
    public AWSCredentialsBean(String keyId, String secretKey)
    {
        accessKeyId = keyId;
        secretAccessKey = secretKey;
    }
    
    public static AWSCredentials getCredentials()
    {
        return new BasicAWSCredentials(accessKeyId, secretAccessKey);
    }
    
    
}
