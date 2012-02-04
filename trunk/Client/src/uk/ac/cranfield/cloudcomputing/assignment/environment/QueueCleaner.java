package uk.ac.cranfield.cloudcomputing.assignment.environment;

import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;


public class QueueCleaner
{
    
    private static final String accessKeyId = "AKIAJ2KOCJHIWA4JVTYQ";
    private static final String secretAccessKey = "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";
    public static final String ENDPOINT_ZONE = "sqs.eu-west-1.amazonaws.com";
    private AmazonSQSClient sqsClient;
    private AWSCredentials credentials;
    
    public static final String DATA_QUEUE = "matDataQueue";
    public static final String RESULT_QUEUE = "matResultQueue";
    public static final String workerQueue = "i-c0f08e89workerQueue";
    
    public QueueCleaner()
    {
        
        credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        
        sqsClient = new AmazonSQSClient(credentials);
        sqsClient.setEndpoint(ENDPOINT_ZONE);
    }
    
    
    public void clearQueue(String name, Integer n)
    {
        
        
        CreateQueueRequest c = new CreateQueueRequest(name);
        CreateQueueResult queueResult = sqsClient.createQueue(c);
        String queueURL = queueResult.getQueueUrl();
        
        do
        {
            ReceiveMessageRequest rmr = new ReceiveMessageRequest(queueURL);
            ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
            List<Message> messages = result.getMessages();
            
            if (messages.size() > 0)
            {
                DeleteMessageRequest delMes = new DeleteMessageRequest(queueURL, messages.get(0).getReceiptHandle());
                sqsClient.deleteMessage(delMes);
                n--;
            }
            
        } while (n > 0);
    }
    
    public static void main(String[] arg)
    {
        QueueCleaner q = new QueueCleaner();
        
        // q.clearQueue("i-9c3177d5_matWorkerQueue", 1);
        q.clearQueue(DATA_QUEUE, 1);
        // q.clearQueue(RESULT_QUEUE, 3);
        
    }
}
