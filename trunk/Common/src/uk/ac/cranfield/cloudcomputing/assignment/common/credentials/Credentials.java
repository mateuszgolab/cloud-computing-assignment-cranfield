package uk.ac.cranfield.cloudcomputing.assignment.common.credentials;


public class Credentials
{
    
    private static String accessKeyId = "AKIAJ2KOCJHIWA4JVTYQ";
    private static String secretAccessKey = "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";
    
    public Credentials(String keyId, String secretKey)
    {
        accessKeyId = keyId;
        secretAccessKey = secretKey;
    }
    
    /**
     * @return the accessKeyId
     */
    public final static String getAccessKeyId()
    {
        return accessKeyId;
    }
    
    
    /**
     * @return the secretAccessKey
     */
    public final static String getSecretAccessKey()
    {
        return secretAccessKey;
    }
    
    
}
