package uk.ac.cranfield.cloudcomputing.assignment.worker;

import java.util.List;

import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixAdditionDataChunk;

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


public class Worker
{
    
    private static final String accessKeyId = "AKIAJ2KOCJHIWA4JVTYQ";
    private static final String secretAccessKey = "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";
    private AmazonSQSClient sqsClient;
    private AWSCredentials credentials;
    public static final String ENDPOINT_ZONE = "sqs.eu-west-1.amazonaws.com";
    private String dataQueueURL;
    private String resultQueueURL;
    public static final Integer NUMBER_OF_ITERATIONS = 100;
    private Integer iterations;
    public static final Integer WAIT_IN_MS = 1;
    private long time;
    
    
    public Worker()
    {
        credentials = null;
        sqsClient = null;
        credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        
        sqsClient = new AmazonSQSClient(credentials);
        sqsClient.setEndpoint(ENDPOINT_ZONE);
        
        sqsClient = new AmazonSQSClient(credentials);
        sqsClient.setEndpoint(ENDPOINT_ZONE);
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
    
    
    public void receive()
    {
        try
        {
            do
            {
                ReceiveMessageRequest rmr = new ReceiveMessageRequest(dataQueueURL);
                rmr.setMaxNumberOfMessages(1);
                
                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                List<Message> messages = result.getMessages();

                if (messages.size() > 0)
                {
                    Message m = messages.get(0);
                    
                    if ("END".compareToIgnoreCase(m.getBody()) == 0)
                    {
                        DeleteMessageRequest delMes = new DeleteMessageRequest(dataQueueURL, m.getReceiptHandle());
                        sqsClient.deleteMessage(delMes);
                        return;
                    }
                    
                    // System.out.println("Received : " + m.getBody());
                    String res = processDataAddition(m);
                    SendMessageRequest smr = new SendMessageRequest(resultQueueURL, res);
                    sqsClient.sendMessage(smr);
                    
                    DeleteMessageRequest delMes = new DeleteMessageRequest(dataQueueURL, m.getReceiptHandle());
                    sqsClient.deleteMessage(delMes);
                }
                Thread.sleep(WAIT_IN_MS);
                
            } while (true);
            
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    
    public void removeQueue(String name)
    {
        CreateQueueRequest c = new CreateQueueRequest(name);
        CreateQueueResult queueResult = sqsClient.createQueue(c);
        DeleteQueueRequest del = new DeleteQueueRequest(queueResult.getQueueUrl());
        sqsClient.deleteQueue(del);
        
    }
    
    
    public String processDataAddition(Message m)
    {
        String data = m.getBody();
        String[] values = data.split(MatrixAdditionDataChunk.separator);
        String result = "";
        
        Integer size = Integer.parseInt(values[1]);
        Integer j = 0;
        Integer tmp = 0;
        
        for (j = 0; j < 2; j++)
            result += values[j] + MatrixAdditionDataChunk.separator;
        
        
        for (int i = 0; i < size; i++)
        {
            tmp = Integer.parseInt(values[i + j]) + Integer.parseInt(values[i + j + size]);
            result += tmp.toString() + MatrixAdditionDataChunk.separator;
        }
        
        return result;
        
        
    }
    
    public String processDataMultiplication(Message m)
    {
        String data = m.getBody();
        String[] values = data.split(MatrixAdditionDataChunk.separator);
        String result = "";
        
        Integer size = Integer.parseInt(values[1]);
        Integer j = 0;
        Integer tmp = 0;
        
        for (j = 0; j < 2; j++)
            result += values[j] + MatrixAdditionDataChunk.separator;
        
        
        for (int i = 0; i < size; i++)
        {
            tmp = Integer.parseInt(values[i + j]) + Integer.parseInt(values[i + j + size]);
            result += tmp.toString() + MatrixAdditionDataChunk.separator;
        }
        
        return result;
        
        
    }


}
