package uk.ac.cranfield.cloudcomputing.assignment.master;

import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixDataChunk;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class MatrixDataUploader extends Thread
{
    
    private static final String accessKeyId = "AKIAJ2KOCJHIWA4JVTYQ";
    private static final String secretAccessKey = "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";
    public static final String ENDPOINT_ZONE = "sqs.eu-west-1.amazonaws.com";
    
    protected String queueURL;
    private String queueName;
    protected AmazonSQSClient sqsClient;
    private AWSCredentials credentials;
    protected Matrix matrix;
    
    public MatrixDataUploader(Matrix matrix, String queueName, AWSCredentials credentials)
    {
        this.queueName = queueName;
        this.credentials = credentials;
        this.matrix = matrix;
        
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
            MatrixDataChunk chunk = new MatrixDataChunk(i, matrix.getSize(), matrix.getRow(i));
            SendMessageRequest smr = new SendMessageRequest(queueURL, chunk.toString());
            sqsClient.sendMessage(smr);
        }
    }
    
}
