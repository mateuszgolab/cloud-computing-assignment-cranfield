package uk.ac.cranfield.cloudcomputing.assignment.matrix;

import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixRowDataChunk;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class MatrixUploaderThread extends Thread
{
    
    private static final String accessKeyId = "AKIAJ2KOCJHIWA4JVTYQ";
    private static final String secretAccessKey = "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";
    public static final String ENDPOINT_ZONE = "sqs.eu-west-1.amazonaws.com";
    
    private Matrix matrix;
    private String queueURL;
    private String queueName;
    private AmazonSQSClient sqsClient;
    private AWSCredentials credentials;
    
    public MatrixUploaderThread(Integer size, Integer key, String queueName)
    {
        matrix = new Matrix(size);
        matrix.generateRandomValues(key);
        this.queueName = queueName;
        
        credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        
        sqsClient = new AmazonSQSClient(credentials);
        sqsClient.setEndpoint(ENDPOINT_ZONE);
        
    }
    
    public void connectQueue()
    {
        try
        {
            CreateQueueRequest c = new CreateQueueRequest(queueName);
            CreateQueueResult queueResult = sqsClient.createQueue(c);
            queueURL = queueResult.getQueueUrl();
            
        }
        catch (AmazonServiceException ase)
        {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:        " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:           " + ase.getErrorType());
            System.out.println("Request ID:           " + ase.getRequestId());
        }
        catch (AmazonClientException ace)
        {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with SQS, such as not "
                    + "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
    
    @Override
    public void run()
    {
        for (int i = 0; i < matrix.getSize(); i++)
        {
            MatrixRowDataChunk chunk = new MatrixRowDataChunk(i, matrix.getSize(), matrix.getRow(i));
            SendMessageRequest smr = new SendMessageRequest(queueURL, chunk.toString());
            sqsClient.sendMessage(smr);
            
        }
        
    }
    
}
