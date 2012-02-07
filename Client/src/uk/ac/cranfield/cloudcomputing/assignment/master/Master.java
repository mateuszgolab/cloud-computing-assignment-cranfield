package uk.ac.cranfield.cloudcomputing.assignment.master;

import java.util.List;

import uk.ac.cranfield.cloudcomputing.assignment.Controller;
import uk.ac.cranfield.cloudcomputing.assignment.common.QueueType;
import uk.ac.cranfield.cloudcomputing.assignment.common.credentials.AWSCredentialsBean;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixResultDataChunk;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Operation;
import uk.ac.cranfield.cloudcomputing.assignment.view.StatusPanel;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
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
    
    public static final String ENDPOINT_ZONE = "sqs.eu-west-1.amazonaws.com";
    public static final Integer WAIT_IN_MS = 1;
    private AmazonSQSClient sqsClient;
    private String dataQueueURL;
    private String resultQueueURL;
    private String messageQueueURL;
    private int rowsReceived;
    private long time;
    private Matrix matrixA;
    private Matrix matrixB;
    private Matrix matrixResult;
    private int key;
    private int numberOfWorkers;
    private StatusPanel status;
    private MatrixOperationExecutor localWorker;
    
    
    public Master(int workers, int size, StatusPanel status, MatrixOperationExecutor worker)
    {
        this.localWorker = worker;
        this.status = status;
        rowsReceived = size;
        numberOfWorkers = workers;
        matrixResult = new Matrix(size);
        
        sqsClient = new AmazonSQSClient(AWSCredentialsBean.getCredentials());
        sqsClient.setEndpoint(ENDPOINT_ZONE);
        
    }
    
    public void connectToQueues(String dataQueue, String resultQueue, String messageQueue)
    {
        
        try
        {
            CreateQueueRequest c = new CreateQueueRequest(dataQueue);
            CreateQueueResult queueResult = sqsClient.createQueue(c);
            dataQueueURL = queueResult.getQueueUrl();
            Controller.incRequest();
            
            c = new CreateQueueRequest(resultQueue);
            queueResult = sqsClient.createQueue(c);
            resultQueueURL = queueResult.getQueueUrl();
            Controller.incRequest();
            
            c = new CreateQueueRequest(messageQueue);
            queueResult = sqsClient.createQueue(c);
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
    
    public Matrix receiveResults()
    {
        
        
        ReceiveMessageRequest rmr = new ReceiveMessageRequest(resultQueueURL);
        rmr.setMaxNumberOfMessages(10);
        
        try
        {
            do
            {
                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                Controller.incRequest();
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    for (Message m : messages)
                    {
                        MatrixResultDataChunk receivedChunk = new MatrixResultDataChunk(m.getBody());
                        matrixResult.setRows(receivedChunk.getMatrixRows(), receivedChunk.getRowIndex());
                        
                        DeleteMessageRequest delMes = new DeleteMessageRequest(resultQueueURL, m.getReceiptHandle());
                        sqsClient.deleteMessage(delMes);
                        Controller.incRequest();
                        rowsReceived -= receivedChunk.getNumberOfRows();
                        int progress = (receivedChunk.getSize() - rowsReceived) * 10000 / receivedChunk.getSize();
                        localWorker.update(progress);
                        // status.print("Received : " + progress + " %");
                    }
                    
                }
                
                Thread.sleep(WAIT_IN_MS);
                
            } while (rowsReceived > 0);
            
            for (int i = 0; i < numberOfWorkers; i++)
            {
                SendMessageRequest smr = new SendMessageRequest(dataQueueURL, Operation.END_OF_CALCULATIONS.toString());
                sqsClient.sendMessage(smr);
                Controller.incRequest();
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
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        finally
        {
            // System.out.println("Parallel cloud  " + matrixA.getSize() + " x " + matrixB.getSize()
            // + " matrix addition time elapsed : " + Integer.toString((int) (System.currentTimeMillis() - time))
            // + " ms");
            // matrixA.print();
            // matrixB.print();
            // matrixResult.print();
            
            return matrixResult;
        }
    }
    
    public void removeQueue(String name)
    {
        CreateQueueRequest c = new CreateQueueRequest(name);
        CreateQueueResult queueResult = sqsClient.createQueue(c);
        Controller.incRequest();
        DeleteQueueRequest del = new DeleteQueueRequest(queueResult.getQueueUrl());
        sqsClient.deleteQueue(del);
        Controller.incRequest();
        
    }
    
    public boolean validate(Matrix m)
    {
        return matrixResult.equals(m);
    }
    
    public void clearQueue(String name, Integer n)
    {
        
        CreateQueueRequest c = new CreateQueueRequest(name);
        CreateQueueResult queueResult = sqsClient.createQueue(c);
        Controller.incRequest();
        String queueURL = queueResult.getQueueUrl();
        
        do
        {
            ReceiveMessageRequest rmr = new ReceiveMessageRequest(queueURL);
            ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
            Controller.incRequest();
            List<Message> messages = result.getMessages();
            
            if (messages.size() > 0)
            {
                DeleteMessageRequest delMes = new DeleteMessageRequest(queueURL, messages.get(0).getReceiptHandle());
                sqsClient.deleteMessage(delMes);
                Controller.incRequest();
                n--;
            }
            
        } while (n > 0);
    }
    
    public String getDataQueueURL()
    {
        return dataQueueURL;
    }
    
    public void sendMessage(Operation op)
    {
        for (int i = 0; i < numberOfWorkers; i++)
        {
            SendMessageRequest smr = new SendMessageRequest(messageQueueURL, op.toString());
            sqsClient.sendMessage(smr);
            Controller.incRequest();
        }
        
    }
    
    public void sendMessage(Operation op, int nodes)
    {
        for (int i = 0; i < nodes; i++)
        {
            SendMessageRequest smr = new SendMessageRequest(dataQueueURL, op.toString());
            sqsClient.sendMessage(smr);
            Controller.incRequest();
        }
        
    }
    
    public void receiveMessages(Operation op, QueueType type)
    {
        
        String queueURL = "";
        
        switch (type) {
            case DATA:
                queueURL = dataQueueURL;
                break;
            case RESULT:
                queueURL = resultQueueURL;
                break;
            case MESSAGE:
                queueURL = messageQueueURL;
                break;
        }
        
        ReceiveMessageRequest rmr = new ReceiveMessageRequest(queueURL);
        rmr.setMaxNumberOfMessages(10);
        int n = numberOfWorkers;
        
        try
        {
            
            do
            {
                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                Controller.incRequest();
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    for (Message m : messages)
                    {
                        if (op.toString().compareToIgnoreCase(m.getBody()) == 0)
                        {
                            DeleteMessageRequest delMes = new DeleteMessageRequest(queueURL, m.getReceiptHandle());
                            sqsClient.deleteMessage(delMes);
                            Controller.incRequest();
                            n--;
                        }
                    }
                }
                
                Thread.sleep(WAIT_IN_MS);
                
                
            } while (n > 0);
            
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    
    public Integer receiveEndingMessages(QueueType type)
    {
        
        String queueURL = "";
        
        switch (type) {
            case DATA:
                queueURL = dataQueueURL;
                break;
            case RESULT:
                queueURL = resultQueueURL;
                break;
            case MESSAGE:
                queueURL = messageQueueURL;
                break;
        }
        
        ReceiveMessageRequest rmr = new ReceiveMessageRequest(queueURL);
        rmr.setMaxNumberOfMessages(10);
        int n = numberOfWorkers;
        Integer cost = 0;
        
        try
        {
            
            do
            {
                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                Controller.incRequest();
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    for (Message m : messages)
                    {
                        String[] s = m.getBody().split(",");
                        
                        if (Operation.CONFIRMATION.toString().compareToIgnoreCase(s[0]) == 0)
                        {
                            DeleteMessageRequest delMes = new DeleteMessageRequest(queueURL, m.getReceiptHandle());
                            sqsClient.deleteMessage(delMes);
                            Controller.incRequest();
                            n--;
                            cost += Integer.parseInt(s[1]);
                        }
                    }
                }
                
                Thread.sleep(WAIT_IN_MS);
                
                
            } while (n > 0);
            
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        
        return cost;
    }
    
    public void removeWorkerQueues(List<String> queueURLs)
    {
        for (String url : queueURLs)
        {
            DeleteQueueRequest del = new DeleteQueueRequest(url);
            sqsClient.deleteQueue(del);
            Controller.incRequest();
        }
    }
    
    public void sendMessageToWorkers(Operation op)
    {
        for (int i = 0; i < numberOfWorkers; i++)
        {
            SendMessageRequest smr = new SendMessageRequest(messageQueueURL, op.toString());
            sqsClient.sendMessage(smr);
            Controller.incRequest();
        }
    }
}
