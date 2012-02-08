package uk.ac.cranfield.cloudcomputing.assignment.common.credentials;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;


public class AWSCredentialsBean
{
    
    private static String accessKeyId = "***";
    private static String secretAccessKey = "***";
    
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
