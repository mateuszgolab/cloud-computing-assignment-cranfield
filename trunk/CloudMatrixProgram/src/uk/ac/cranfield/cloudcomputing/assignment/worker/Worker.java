package uk.ac.cranfield.cloudcomputing.assignment.worker;

import java.util.ArrayList;
import java.util.List;

import uk.ac.cranfield.cloudcomputing.assignment.common.credentials.AWSCredentialsBean;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixDataChunk;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixDoubleDataChunk;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixResultDataChunk;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Operation;

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


public class Worker
{
    
    public static final String ENDPOINT_ZONE = "sqs.eu-west-1.amazonaws.com";
    public static final Integer WAIT_IN_MS = 1;
    public static final Integer POOLING_WAITING_TIME_IN_MS = 100;
    private AmazonSQSClient sqsClient;
    private String dataQueueURL;
    private String resultQueueURL;
    private String workerQueueURL;
    private String messageQueueURL;
    private Matrix matrixB;
    private int rowsToReceive;
    
    
    public Worker()
    {
        sqsClient = new AmazonSQSClient(AWSCredentialsBean.getCredentials());
        sqsClient.setEndpoint(ENDPOINT_ZONE);
        
    }
    
    public void connectToQueues(String dataQueue, String resultQueue, String messageQueue, String workerQueue)
            throws AmazonServiceException
    {
        
        try
        {
            CreateQueueRequest c = new CreateQueueRequest(dataQueue);
            CreateQueueResult queueResult = sqsClient.createQueue(c);
            dataQueueURL = queueResult.getQueueUrl();
            Main.incRequest();
            
            c = new CreateQueueRequest(resultQueue);
            queueResult = sqsClient.createQueue(c);
            resultQueueURL = queueResult.getQueueUrl();
            Main.incRequest();
            

            c = new CreateQueueRequest(workerQueue);
            queueResult = sqsClient.createQueue(c);
            workerQueueURL = queueResult.getQueueUrl();
            Main.incRequest();
            
            c = new CreateQueueRequest(messageQueue);
            queueResult = sqsClient.createQueue(c);
            messageQueueURL = queueResult.getQueueUrl();
            Main.incRequest();


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
        Main.incRequest();
    }
    
    private List<String> processDataAddition(MatrixDoubleDataChunk chunk)
    {
        List<Integer[]> rowsA = chunk.getMatrixRows();
        List<Integer[]> rowsB = chunk.getMatrixBRows();
        List<Integer[]> resultRows = new ArrayList<Integer[]>();
        List<String> resultChunks = new ArrayList<String>();
        int rowsSize = 0;
        int rowIndex = chunk.getRowIndex();
        int rowsCount = 0;

        
        for (int i = 0; i < chunk.getNumberOfRows(); i++)
        {
            Integer[] rowA = rowsA.get(i);
            Integer[] rowB = rowsB.get(i);
            
            for (int j = 0; j < chunk.getSize(); j++)
            {
                rowA[j] += rowB[j];
            }
            
            int size = MatrixDataChunk.getRowLength(rowA);
            
            if (rowsSize + size > MatrixDataChunk.SIZE_LIMIT)
            {
                resultChunks
                        .add(new MatrixResultDataChunk(rowsCount, rowIndex, chunk.getSize(), resultRows)
                        .toString());
                
                rowIndex += rowsCount;
                resultRows = new ArrayList<Integer[]>();
                resultRows.add(rowA);
                rowsSize = size;
                rowsCount = 1;
                
            }
            else
            {
                rowsCount++;
                rowsSize += size;
                resultRows.add(rowA);
            }
            
        }

        if (resultChunks.size() > 0)
        {
            resultChunks.add(new MatrixResultDataChunk(rowsCount, rowIndex, chunk.getSize(), resultRows)
                    .toString());
        }
        else
        {
            resultChunks.add(new MatrixResultDataChunk(chunk.getNumberOfRows(), chunk.getRowIndex(), chunk.getSize(),
                    resultRows).toString());
        }
        
        return resultChunks;
    }
    
    private List<String> processDataMultiplication(MatrixDataChunk chunk)
    {
        List<Integer[]> rows = chunk.getMatrixRows();
        List<Integer[]> allRowsResults = new ArrayList<Integer[]>();
        int totalResultSize = 0;
        int rowsNumber = 0;
        int rowIndex = chunk.getRowIndex();
        List<Integer[]> rowsResult = new ArrayList<Integer[]>();
        List<String> chunksResult = new ArrayList<String>();
        
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
            
       
            int size = MatrixDataChunk.getRowLength(resultRow);
            
            if(totalResultSize + size <= MatrixDataChunk.SIZE_LIMIT)
            {
                totalResultSize += size;
                rowsNumber++;
                rowsResult.add(resultRow);
            }
            else
            {
                chunksResult.add(new MatrixResultDataChunk(rowsNumber, rowIndex, chunk.getSize(),
                        rowsResult).toString());
                rowIndex += rowsNumber;

                rowsResult = new ArrayList<Integer[]>();
                rowsNumber = 1;
                totalResultSize = size;
                rowsResult.add(resultRow);
            }
        }
        
        if (chunksResult.size() > 0)
        {
            
            chunksResult.add(new MatrixResultDataChunk(rowsNumber, rowIndex, chunk.getSize(), rowsResult).toString());
        }
        else
        {
            chunksResult.add(new MatrixResultDataChunk(chunk.getNumberOfRows(), chunk.getRowIndex(), chunk.getSize(),
                    rowsResult).toString());
        }
        
        return chunksResult;
        
    }
    
    public Operation receiveStartingMessage() throws AmazonServiceException
    {
        ReceiveMessageRequest request = new ReceiveMessageRequest(messageQueueURL);
        request.setVisibilityTimeout(1);
        
        
        try
        {
            
            do
            {
                ReceiveMessageResult result = sqsClient.receiveMessage(request);
                Main.incRequest();
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    Message m = messages.get(0);
                    
                    if (Operation.END_OF_PROGRAM.toString().compareToIgnoreCase(m.getBody()) == 0)
                    {
                        deleteMessage(messageQueueURL, m);
                        return Operation.END_OF_PROGRAM;
                    }
                    else if (Operation.ADDITION.toString().compareToIgnoreCase(m.getBody()) == 0)
                    {
                        deleteMessage(messageQueueURL, m);
                        return Operation.ADDITION;
                    }
                    else if (Operation.MULTIPLICATION.toString().compareToIgnoreCase(m.getBody()) == 0)
                    {
                        deleteMessage(messageQueueURL, m);
                        return Operation.MULTIPLICATION;
                    }

                }

                Thread.sleep(POOLING_WAITING_TIME_IN_MS);
                
            } while (true);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
            return Operation.END_OF_PROGRAM;
        }
    }
    
    private void deleteMessage(String queueURL, Message m)
    {
        DeleteMessageRequest delMes = new DeleteMessageRequest(queueURL, m.getReceiptHandle());
        sqsClient.deleteMessage(delMes);
        Main.incRequest();
    }

    public void matrixAddition()
    {
        try
        {
            
            ReceiveMessageRequest rmr = new ReceiveMessageRequest(dataQueueURL);
            rmr.setMaxNumberOfMessages(1);

            do
            {
                
                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                Main.incRequest();
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    for (Message m : messages)
                    {
                        if (Operation.END_OF_CALCULATIONS.toString().compareToIgnoreCase(m.getBody()) == 0)
                        {
                            deleteMessage(dataQueueURL, m);
                            sendEndingConfirmation(resultQueueURL);
                            return;
                        }

                        MatrixDoubleDataChunk chunk = new MatrixDoubleDataChunk(m.getBody());
                        deleteMessage(dataQueueURL, m);

                        List<String> res = processDataAddition(chunk);
                        for (String r : res)
                        {
                            SendMessageRequest smr = new SendMessageRequest(resultQueueURL, r);
                            sqsClient.sendMessage(smr);
                            Main.incRequest();
                        }

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

            // receiveing matrix B size

            do
            {

                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                Main.incRequest();
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    Message m = messages.get(0);
                    MatrixDataChunk chunk = new MatrixDataChunk(m.getBody());
                    matrixB = new Matrix(chunk.getSize());
                    matrixB.setRows(chunk.getMatrixRows(), chunk.getRowIndex());
                    deleteMessage(workerQueueURL, m);
                    rowsToReceive = chunk.getSize() - chunk.getNumberOfRows();
                    break;
                }
                Thread.sleep(WAIT_IN_MS);
                
            } while (true);
            
            
            // receiving data for matrix B

            rmr.setMaxNumberOfMessages(10);

            do
            {
                
                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                Main.incRequest();
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
            rmr.setMaxNumberOfMessages(1);
            rmr.setVisibilityTimeout(300);
            

            // receiving data for calculations
            do
            {

                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                Main.incRequest();
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    for (Message m : messages)
                    {
                        if (Operation.END_OF_CALCULATIONS.toString().compareToIgnoreCase(m.getBody()) == 0)
                        {
                            deleteMessage(dataQueueURL, m);
                            sendEndingConfirmation(resultQueueURL);
                            return;
                        }
                        

                        MatrixDataChunk chunk = new MatrixDataChunk(m.getBody());
                        List<String> resultList = processDataMultiplication(chunk);
                        
                        for (String res : resultList)
                        {
                            SendMessageRequest smr = new SendMessageRequest(resultQueueURL, res);
                            sqsClient.sendMessage(smr);
                            Main.incRequest();

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
    
    public void sendConfirmation(String queueURL)
    {
        SendMessageRequest smr = new SendMessageRequest(queueURL, Operation.CONFIRMATION.toString());
        sqsClient.sendMessage(smr);
        Main.incRequest();
    }
    
    public void sendEndingConfirmation(String queueURL)
    {
        SendMessageRequest smr = new SendMessageRequest(queueURL, Operation.CONFIRMATION.toString() + ","
                + Main.getRequests());
        sqsClient.sendMessage(smr);
        Main.incRequest();
    }
    
    public void sendConfirmation()
    {
        SendMessageRequest smr = new SendMessageRequest(resultQueueURL, Operation.CONFIRMATION.toString());
        sqsClient.sendMessage(smr);
        Main.incRequest();
    }

    public void resendMessage(String queueUrl, String body)
    {
        SendMessageRequest smr = new SendMessageRequest(queueUrl, body);
        sqsClient.sendMessage(smr);
        Main.incRequest();
    }

    private void waitForOtherWorkers()
    {
        ReceiveMessageRequest rmr = new ReceiveMessageRequest(messageQueueURL);
        
        try
        {
            do
            {
                ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
                Main.incRequest();
                List<Message> messages = result.getMessages();
                
                if (messages.size() > 0)
                {
                    Message m = messages.get(0);
                    
                    if (Operation.CONFIRMATION.toString().compareToIgnoreCase(m.getBody()) == 0)
                    {
                        DeleteMessageRequest delMes = new DeleteMessageRequest(messageQueueURL, m.getReceiptHandle());
                        sqsClient.deleteMessage(delMes);
                        Main.incRequest();
                        break;
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
}
