package uk.ac.cranfield.cloudcomputing.assignment.master;

import java.util.ArrayList;
import java.util.List;

import uk.ac.cranfield.cloudcomputing.assignment.common.credentials.AWSCredentialsBean;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixDataChunk;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Operation;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class MatrixDataUploader
{
    
    public static final String ENDPOINT_ZONE = "sqs.eu-west-1.amazonaws.com";
    
    protected List<String> queuesURLs;
    private List<String> queuesNames;
    protected AmazonSQSClient sqsClient;
    protected Matrix matrix;
    protected int numberOfDataBlocks;
    
    public MatrixDataUploader(Matrix matrix, List<String> queues, int numberOfDataBlocks)
    {
        this.numberOfDataBlocks = numberOfDataBlocks;
        this.queuesNames = queues;
        this.matrix = matrix;
        this.queuesURLs = new ArrayList<String>();
        
        sqsClient = new AmazonSQSClient(AWSCredentialsBean.getCredentials());
        sqsClient.setEndpoint(ENDPOINT_ZONE);
        
    }
    
    public MatrixDataUploader(Matrix matrix, String queueURL, int numberOfDataBlocks)
    {
        this.numberOfDataBlocks = numberOfDataBlocks;
        this.matrix = matrix;
        this.queuesURLs = new ArrayList<String>();
        this.queuesURLs.add(queueURL);
        
        sqsClient = new AmazonSQSClient(AWSCredentialsBean.getCredentials());
        sqsClient.setEndpoint(ENDPOINT_ZONE);
        
    }
    
    public void connectToQueue()
    {
        try
        {
            for (String q : queuesNames)
            {
                CreateQueueRequest c = new CreateQueueRequest(q);
                CreateQueueResult queueResult = sqsClient.createQueue(c);
                queuesURLs.add(queueResult.getQueueUrl());
            }
            
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
    
    public void send()
    {
        List<MatrixDataChunk> chunks = matrix.decompose(numberOfDataBlocks);
        
        for (MatrixDataChunk chunk : chunks)
        {
            String data = chunk.toString();
            for (String queue : queuesURLs)
            {
                SendMessageRequest smr = new SendMessageRequest(queue, data);
                sqsClient.sendMessage(smr);
            }
        }
        
    }
    
    public void sendMessageToWorkers(Operation op)
    {
        for (String url : queuesURLs)
        {
            SendMessageRequest smr = new SendMessageRequest(url, op.toString());
            sqsClient.sendMessage(smr);
        }
    }
}
