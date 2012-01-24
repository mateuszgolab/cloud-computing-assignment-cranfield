package uk.ac.cranfield.cloudcomputing.assignment.master;

import java.util.List;

import uk.ac.cranfield.cloudcomputing.assignment.common.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.MatrixAdditionDataChunk;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class Master
{
    
    private static final String accessKeyId = "AKIAJ2KOCJHIWA4JVTYQ";
    private static final String secretAccessKey = "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";
    public static final String ENDPOINT_ZONE = "sqs.eu-west-1.amazonaws.com";
    public static final Integer WAIT_IN_MS = 1;
    private AmazonSQSClient sqsClient;
    private AWSCredentials credentials;
    private String dataQueueURL;
    private String resultQueueURL;
    private Integer iterations;
    private long time;
    private Matrix matrixA;
    private Matrix matrixB;
    private Matrix matrixResult;
    private Integer key;

    
    public Master()
    {
        iterations = 0;

        credentials = null;
        sqsClient = null;
        credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        
        sqsClient = new AmazonSQSClient(credentials);
        sqsClient.setEndpoint(ENDPOINT_ZONE);
        
        sqsClient = new AmazonSQSClient(credentials);
        sqsClient.setEndpoint(ENDPOINT_ZONE);
    }
    
    public Master(Matrix a, Matrix b, Integer k)
    {
        this();
        matrixA = a;
        matrixB = b;
        key = k;
        iterations = a.getSize();
    }

    public void connectQueues(String dataQueue, String resultQueue)
    {
        
        try
        {
            CreateQueueRequest c = new CreateQueueRequest(dataQueue);
            CreateQueueResult queueResult = sqsClient.createQueue(c);
            dataQueueURL = queueResult.getQueueUrl();
            
            c = new CreateQueueRequest(resultQueue);
            queueResult = sqsClient.createQueue(c);
            resultQueueURL = queueResult.getQueueUrl();

            
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
    
    
    public void distributeData()
    {
        
        time = System.currentTimeMillis();
        
        for (int i = 0; i < matrixA.getSize(); i++)
        {
            MatrixAdditionDataChunk chunk = new MatrixAdditionDataChunk(matrixA.getRow(i), matrixB.getRow(i), i, key);
            SendMessageRequest smr = new SendMessageRequest(dataQueueURL, chunk.toString());
            sqsClient.sendMessage(smr);

        }

        receiveResults();

    }
    
    private void receiveResults()
    {
        try
        {
            do
            {
                ReceiveMessageRequest rmr = new ReceiveMessageRequest(resultQueueURL);
                rmr.setMaxNumberOfMessages(1);
                
                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    Message m = messages.get(0);
                    String data = m.getBody();
                    matrixResult.setRow(getRowIndex(data), getRow(data));

                    DeleteMessageRequest delMes = new DeleteMessageRequest(resultQueueURL, m.getReceiptHandle());
                    sqsClient.deleteMessage(delMes);

                    rowReceived();
                }

                Thread.sleep(WAIT_IN_MS);
                
            } while (getIteration() > 0);
            
            SendMessageRequest smr = new SendMessageRequest(dataQueueURL, "END");
            sqsClient.sendMessage(smr);
            
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        finally
        {
            System.out.println("Time elapsed : " + Integer.toString((int) (System.currentTimeMillis() - time)) + " ms");
        }
    }
    
    public void removeQueue(String name)
    {
        CreateQueueRequest c = new CreateQueueRequest(name);
        CreateQueueResult queueResult = sqsClient.createQueue(c);
        DeleteQueueRequest del = new DeleteQueueRequest(queueResult.getQueueUrl());
        sqsClient.deleteQueue(del);
        
    }
    
    private Integer[] getRow(String data)
    {
        Integer key = (int) data.charAt(0);
        Integer size = (int) data.charAt(2);
        Integer[] sum = new Integer[size];
        
        
        Integer index = 3;
        
        for (int i = 0; i < size; i++)
        {
            sum[i] = data.charAt(index + i) - key;
        }
        
        return sum;
    }
    
    private Integer getRowIndex(String data)
    {
        return (int) data.charAt(1);
    }


    private synchronized void rowReceived()
    {
        iterations--;
    }
    
    private synchronized Integer getIteration()
    {
        return iterations;
    }
    
    

}
