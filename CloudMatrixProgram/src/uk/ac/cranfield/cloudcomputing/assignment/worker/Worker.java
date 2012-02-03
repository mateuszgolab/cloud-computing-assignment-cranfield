package uk.ac.cranfield.cloudcomputing.assignment.worker;

import java.util.ArrayList;
import java.util.List;

import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixDataChunk;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixDoubleDataChunk;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixResultDataChunk;
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


public class Worker
{
    
    private static final String accessKeyId = "AKIAJ2KOCJHIWA4JVTYQ";
    private static final String secretAccessKey = "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";
    public static final String ENDPOINT_ZONE = "sqs.eu-west-1.amazonaws.com";
    public static final Integer WAIT_IN_MS = 1;
    private AmazonSQSClient sqsClient;
    private AWSCredentials credentials;
    private String dataQueueURL;
    private String resultQueueURL;
    private String workerQueueURL;
    private Message firstMessage;
    private Matrix matrixB;
    private int rowsToReceive;
    
    
    public Worker()
    {


        credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        
        sqsClient = new AmazonSQSClient(credentials);
        sqsClient.setEndpoint(ENDPOINT_ZONE);
        
    }
    
    public void connectToQueues(String dataQueue, String resultQueue, String workerQueue)
            throws AmazonServiceException
    {
        
        try
        {
            CreateQueueRequest c = new CreateQueueRequest(dataQueue);
            CreateQueueResult queueResult = sqsClient.createQueue(c);
            dataQueueURL = queueResult.getQueueUrl();
            
            c = new CreateQueueRequest(resultQueue);
            queueResult = sqsClient.createQueue(c);
            resultQueueURL = queueResult.getQueueUrl();
            

            c = new CreateQueueRequest(workerQueue);
            queueResult = sqsClient.createQueue(c);
            workerQueueURL = queueResult.getQueueUrl();
            
        }
        catch (AmazonClientException ace)
        {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with SQS, such as not "
                    + "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        
    }
    
    public void removeWorkerQueue()
    {
        DeleteQueueRequest del = new DeleteQueueRequest(workerQueueURL);
        sqsClient.deleteQueue(del);
    }
    
    public String processDataAddition(MatrixDoubleDataChunk chunk)
    {
        List<Integer[]> rowsA = chunk.getMatrixRows();
        List<Integer[]> rowsB = chunk.getMatrixBRows();
        List<Integer[]> resultRows = new ArrayList<Integer[]>();
        
        for (int i = 0; i < chunk.getNumberOfRows(); i++)
        {
            Integer[] rowA = rowsA.get(i);
            Integer[] rowB = rowsB.get(i);
            
            for (int j = 0; j < chunk.getSize(); j++)
            {
                rowA[j] += rowB[j];
            }
            
            resultRows.add(rowA);
        }
        
        return new MatrixResultDataChunk(chunk.getNumberOfRows(), chunk.getRowIndex(), chunk.getSize(), resultRows)
                .toString();
    }
    
    public List<String> processDataMultiplication(MatrixDataChunk chunk)
    {
        List<Integer[]> rows = chunk.getMatrixRows();
        List<Integer[]> allRowsResults = new ArrayList<Integer[]>();
        
        for (Integer[] row : rows)
        {
            Integer[] resultRow = new Integer[chunk.getSize()];
            for (int i = 0; i < matrixB.getSize(); i++)
            {
                resultRow[i] = 0;
                for (int j = 0; j < chunk.getSize(); j++)
                {
                    resultRow[i] += row[j] * matrixB.getValue(j, i);
                }
            }
            
            allRowsResults.add(resultRow);
        }
        
        int totalResultSize = 0;
        int rowsNumber = 0;
        int rowIndex = 0;
        List<Integer[]> rowsResult = new ArrayList<Integer[]>();
        List<MatrixResultDataChunk> chunksResult = new ArrayList<MatrixResultDataChunk>();
        
        for(Integer[] in : allRowsResults)
        {
            int size = MatrixDataChunk.getRowLength(in);
            
            if(totalResultSize + size <= MatrixDataChunk.SIZE_LIMIT)
            {
                totalResultSize += size;
                rowsNumber++;
                rowsResult.add(in);
            }
            else
            {
                chunksResult.add(new MatrixResultDataChunk(rowsNumber, rowIndex, chunk.getSize(), rowsResult));
                rowsResult = new ArrayList<Integer[]>();
                rowIndex = rowsNumber;
                totalResultSize = 0;
                rowsNumber = 0;
                
            }
        }
        
        chunksResult.add(new MatrixResultDataChunk(rowsNumber, rowIndex, chunk.getSize(), rowsResult));
        
        List<String> finalResults = new ArrayList<String>();
        for (MatrixDataChunk ch : chunksResult)
        {
            finalResults.add(ch.toString());
        }
        
        return finalResults;
        
    }
    
    public Operation receiveStartingMessage() throws AmazonServiceException
    {
        ReceiveMessageRequest additionRequest = new ReceiveMessageRequest(dataQueueURL);
        
        
        try
        {
            
            do
            {
                ReceiveMessageResult result = sqsClient.receiveMessage(additionRequest);
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    Message m = messages.get(0);
                    
                    if (Operation.END_PROGRAM.toString().compareToIgnoreCase(m.getBody()) == 0)
                    {
                        deleteMessage(dataQueueURL, m);
                        return Operation.END_PROGRAM;
                    }
                    else if (Operation.ADDITION.toString().compareToIgnoreCase(m.getBody()) == 0)
                    {
                        deleteMessage(dataQueueURL, m);
                        return Operation.ADDITION;
                    }
                    else if (Operation.MULTIPLICATION.toString().compareToIgnoreCase(m.getBody()) == 0)
                    {
                        deleteMessage(dataQueueURL, m);
                        return Operation.MULTIPLICATION;
                    }

                }

                Thread.sleep(1);
                
            } while (true);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
            return Operation.END_PROGRAM;
        }
    }
    
    private void deleteMessage(String queueURL, Message m)
    {
        DeleteMessageRequest delMes = new DeleteMessageRequest(queueURL, m.getReceiptHandle());
        sqsClient.deleteMessage(delMes);
    }

    public void matrixAddition()
    {
        try
        {
            
            ReceiveMessageRequest rmr = new ReceiveMessageRequest(dataQueueURL);
            rmr.setMaxNumberOfMessages(10);

            do
            {
                
                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    for (Message m : messages)
                    {
                        if (Operation.END_CALCULATIONS.toString().compareToIgnoreCase(m.getBody()) == 0)
                        {
                            deleteMessage(dataQueueURL, m);
                            sendConfirmation();
                            return;
                        }

                        MatrixDoubleDataChunk chunk = new MatrixDoubleDataChunk(m.getBody());
                        String res = processDataAddition(chunk);
                        SendMessageRequest smr = new SendMessageRequest(resultQueueURL, res);
                        sqsClient.sendMessage(smr);

                        deleteMessage(dataQueueURL, m);
                    }
                }
                Thread.sleep(WAIT_IN_MS);
                
            } while (true);
            
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    
    public void matrixMultiplication()
    {
        try
        {
            ReceiveMessageRequest rmr = new ReceiveMessageRequest(workerQueueURL);
            rmr.setMaxNumberOfMessages(10);
            
            
            // receiveing matrix B size

            do
            {

                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    for (Message m : messages)
                    {
                        
                        MatrixDataChunk chunk = new MatrixDataChunk(m.getBody());
                        matrixB = new Matrix(chunk.getSize());
                        matrixB.setRows(chunk.getMatrixRows(), chunk.getRowIndex());
                        deleteMessage(workerQueueURL, m);
                        rowsToReceive = chunk.getSize() - chunk.getNumberOfRows();
                    }
                    break;
                }
                Thread.sleep(WAIT_IN_MS);
                
            } while (true);
            
            
            // receiving data for matrix B

            do
            {
                
                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    for (Message m : messages)
                    {
                        
                        MatrixDataChunk chunk = new MatrixDataChunk(m.getBody());
                        matrixB.setRows(chunk.getMatrixRows(), chunk.getRowIndex());
                        deleteMessage(workerQueueURL, m);
                        rowsToReceive -= chunk.getNumberOfRows();
                    }
                }
                Thread.sleep(WAIT_IN_MS);

            } while (rowsToReceive > 0);


            rmr = new ReceiveMessageRequest(dataQueueURL);
            rmr.setMaxNumberOfMessages(10);

            
            // receiving data for calculations
            do
            {

                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    for (Message m : messages)
                    {
                        if (Operation.END_CALCULATIONS.toString().compareToIgnoreCase(m.getBody()) == 0)
                        {
                            deleteMessage(dataQueueURL, m);
                            sendConfirmation();
                            return;
                        }
                        

                        MatrixDataChunk chunk = new MatrixDataChunk(m.getBody());
                        List<String> resultList = processDataMultiplication(chunk);
                        
                        for (String res : resultList)
                        {
                            SendMessageRequest smr = new SendMessageRequest(resultQueueURL, res);
                            sqsClient.sendMessage(smr);
                        }

                        deleteMessage(dataQueueURL, m);
                    }
                }
                Thread.sleep(WAIT_IN_MS);
                
            } while (true);
            
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    
    public Message getFirstMessage()
    {
        return firstMessage;
    }

    public void sendConfirmation()
    {
        SendMessageRequest smr = new SendMessageRequest(resultQueueURL, Operation.CONFIRMATION.toString());
        sqsClient.sendMessage(smr);
    }
}
