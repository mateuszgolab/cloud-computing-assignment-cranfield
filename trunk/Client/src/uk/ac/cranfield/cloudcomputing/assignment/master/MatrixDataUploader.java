package uk.ac.cranfield.cloudcomputing.assignment.master;

import java.util.ArrayList;
import java.util.List;

import javax.swing.JOptionPane;

import uk.ac.cranfield.cloudcomputing.assignment.Controller;
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
    protected String messageQueue;
    protected String messageQueueURL;
    private List<String> queuesNames;
    protected AmazonSQSClient sqsClient;
    protected Matrix matrix;
    protected int numberOfDataBlocks;
    List<MatrixDataChunk> chunks;
    
    public MatrixDataUploader(Matrix matrix, List<String> queues, String messageQueue, int numberOfDataBlocks)
    {
        this.numberOfDataBlocks = numberOfDataBlocks;
        this.queuesNames = queues;
        this.messageQueue = messageQueue;
        this.matrix = matrix;
        this.queuesURLs = new ArrayList<String>();
        
        sqsClient = new AmazonSQSClient(AWSCredentialsBean.getCredentials());
        sqsClient.setEndpoint(ENDPOINT_ZONE);
        
        chunks = matrix.decompose(numberOfDataBlocks);
        
    }
    
    public MatrixDataUploader(Matrix matrix, String queueURL, int numberOfDataBlocks)
    {
        this.numberOfDataBlocks = numberOfDataBlocks;
        this.matrix = matrix;
        this.queuesURLs = new ArrayList<String>();
        this.queuesURLs.add(queueURL);
        
        sqsClient = new AmazonSQSClient(AWSCredentialsBean.getCredentials());
        sqsClient.setEndpoint(ENDPOINT_ZONE);
        
        chunks = matrix.decompose(numberOfDataBlocks);
        
        
        if (chunks.size() != numberOfDataBlocks)
            JOptionPane.showMessageDialog(null, "Matrix is too big to be divided into " + numberOfDataBlocks
                    + " parts. It was divided into " + chunks.size() + " data blocks",
                    "Data blocks number relcalculated", JOptionPane.WARNING_MESSAGE);
        
        
    }
    
    public void connectToQueue()
    {
        try
        {
            for (String q : queuesNames)
            {
                CreateQueueRequest c = new CreateQueueRequest(q);
                CreateQueueResult queueResult = sqsClient.createQueue(c);
                Controller.incRequest();
                queuesURLs.add(queueResult.getQueueUrl());
                Controller.incRequest();
            }
            
            CreateQueueRequest c = new CreateQueueRequest(messageQueue);
            CreateQueueResult queueResult = sqsClient.createQueue(c);
            Controller.incRequest();
            messageQueueURL = queueResult.getQueueUrl();
            Controller.incRequest();
            
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
        
        
        for (MatrixDataChunk chunk : chunks)
        {
            String data = chunk.toString();
            for (String queue : queuesURLs)
            {
                SendMessageRequest smr = new SendMessageRequest(queue, data);
                sqsClient.sendMessage(smr);
                Controller.incRequest();
            }
        }
        
    }
    
    public void sendMessageToWorkers(Operation op)
    {
        
        for (String s : queuesURLs)
        {
            SendMessageRequest smr = new SendMessageRequest(messageQueueURL, op.toString());
            sqsClient.sendMessage(smr);
            Controller.incRequest();
        }
    }
    
    public List<String> getWorkerQueuesURLs()
    {
        return queuesURLs;
    }
}
