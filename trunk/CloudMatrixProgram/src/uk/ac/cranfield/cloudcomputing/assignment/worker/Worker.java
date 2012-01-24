package uk.ac.cranfield.cloudcomputing.assignment.worker;

import java.util.List;

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
                    String res = processRowsAddition(m);
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
    
    
    public String processRowsAddition(Message m)
    {
        String data = m.getBody();
        Integer key = (int) data.charAt(0);
        Integer size = (int) data.charAt(2);
        Integer[] sum = new Integer[size];
        
        
        String result = data.substring(0, 3);
        
        
        Integer index = 3;
        
        for (int i = 0; i < size; i++)
        {
            sum[i] = data.charAt(index + i) - key;
        }
        
        index += size;
        
        // x - key + y - key + key = x - key + y (so ready to string it)
        for (int i = 0; i < size; i++)
        {
            sum[i] += (int) data.charAt(index + i);
        }
        
        
        for (Integer i : sum)
            result += (char) i.intValue();
        
        return result;
        
        
    }
    
}
