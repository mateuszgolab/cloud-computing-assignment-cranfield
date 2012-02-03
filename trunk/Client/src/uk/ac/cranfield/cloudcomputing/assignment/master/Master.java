package uk.ac.cranfield.cloudcomputing.assignment.master;

import java.util.List;

import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixDataChunk;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Operation;

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
    private int rowsReceived;
    private long time;
    private Matrix matrixA;
    private Matrix matrixB;
    private Matrix matrixResult;
    private int key;
    private int numberOfWorkers;
    
    
    public Master(int workers, int size)
    {
        rowsReceived = size;
        numberOfWorkers = workers;
        credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        matrixResult = new Matrix(size);
        
        sqsClient = new AmazonSQSClient(credentials);
        sqsClient.setEndpoint(ENDPOINT_ZONE);
        
    }
    
    public Master(int workers, Matrix a, Matrix b, Integer k)
    {
        this(workers, a.getSize());
        matrixA = a;
        matrixB = b;
        matrixResult = new Matrix(a.getSize());
        key = k;
    }
    
    public void connectToQueues(String dataQueue, String resultQueue)
    {
        
        try
        {
            CreateQueueRequest c = new CreateQueueRequest(dataQueue);
            CreateQueueResult queueResult = sqsClient.createQueue(c);
            dataQueueURL = queueResult.getQueueUrl();
            
            c = new CreateQueueRequest(resultQueue);
            queueResult = sqsClient.createQueue(c);
            resultQueueURL = queueResult.getQueueUrl();
            
            // String s = "";
            // for (int i = 0; i < 64000; i++)
            // {
            // s += "7";
            // }
            //
            //
            // SendMessageRequest smr = new SendMessageRequest(dataQueueURL, s);
            // System.out.println(s.getBytes("UTF-16").length);
            // sqsClient.sendMessage(smr);
            // System.out.println(s.getBytes("UTF-16").length);
            // sqsClient.sendMessage(smr);
            
            
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
    
    public void matrixAddition()
    {
        
        time = System.currentTimeMillis();
        
        try
        {
            
            for (int i = 0; i < matrixA.getSize(); i++)
            {
                // MatrixAdditionDataChunk chunk = new MatrixAdditionDataChunk(matrixA.getRow(i), matrixB.getRow(i), i,
                // matrixA.getSize());
                // SendMessageRequest smr = new SendMessageRequest(dataQueueURL, chunk.toString());
                // sqsClient.sendMessage(smr);
                
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
        
        // receiveData();
        
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
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    for (Message m : messages)
                    {
                        MatrixDataChunk receivedChunk = new MatrixDataChunk(m.getBody());
                        matrixResult.setRows(receivedChunk.getMatrixRows(), receivedChunk.getRowIndex());
                        
                        DeleteMessageRequest delMes = new DeleteMessageRequest(resultQueueURL, m.getReceiptHandle());
                        sqsClient.deleteMessage(delMes);
                    }
                    
                    rowReceived(messages.size());
                }
                
                Thread.sleep(WAIT_IN_MS);
                
            } while (getMissingRowsNumber() > 0);
            
            for (int i = 0; i < numberOfWorkers; i++)
            {
                SendMessageRequest smr = new SendMessageRequest(dataQueueURL, Operation.END_CALCULATIONS.toString());
                sqsClient.sendMessage(smr);
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
        DeleteQueueRequest del = new DeleteQueueRequest(queueResult.getQueueUrl());
        sqsClient.deleteQueue(del);
        
    }
    
    private synchronized void rowReceived(int rows)
    {
        rowsReceived -= rows;
    }
    
    private synchronized int getMissingRowsNumber()
    {
        return rowsReceived;
    }
    
    public boolean validate(Matrix m)
    {
        return matrixResult.equals(m);
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
    
    public void endProgram()
    {
        for (int i = 0; i < numberOfWorkers; i++)
        {
            SendMessageRequest smr = new SendMessageRequest(dataQueueURL, Operation.END_PROGRAM.toString());
            sqsClient.sendMessage(smr);
            
            
        }
    }
    
    // public boolean isMultiplicationNotificationReceived()
    // {
    // ReceiveMessageRequest rmr = new ReceiveMessageRequest(resultQueueURL);
    // ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
    // List<Message> messages = result.getMessages();
    // boolean res = false;
    //
    // if (messages.size() > 0)
    // {
    // res = Operation.RECEPTION.toString().compareToIgnoreCase(messages.get(0).getBody()) == 0;
    // DeleteMessageRequest delMes = new DeleteMessageRequest(resultQueueURL, messages.get(0).getReceiptHandle());
    // sqsClient.deleteMessage(delMes);
    // }
    // return res;
    // }
    
    
    public String getDataQueueURL()
    {
        return dataQueueURL;
    }
    
}
