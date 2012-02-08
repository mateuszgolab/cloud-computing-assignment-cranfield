package uk.ac.cranfield.cloudcomputing.assignment.worker;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.AmazonServiceException;

/**
 * Main function of the program working on the Cloud environment
 * @author Mateusz Golab
 * @version 1.0
 */
public class Main
{
    
    public static final String DATA_QUEUE = "matDataQueue";
    public static final String RESULT_QUEUE = "matResultQueue";
    public static final String MESSAGE_QUEUE = "matMessageQueue";
    private static String workerQueue;
    private static Integer requestCounter;

    /**
     * Main function of the Worker
     * @param args
     */
    public static void main(String[] args)
    {
        Logger.getLogger("com.amazonaws.request").setLevel(Level.SEVERE);
        
        if (args == null || args.length < 1)
            return;
        workerQueue = args[0] + "_matWorkerQueue";

        
        long waitingTimeInMs = 100;
        requestCounter = 0;
        
        try
        {
            
            Worker worker = new Worker();
            worker.connectToQueues(DATA_QUEUE, RESULT_QUEUE, MESSAGE_QUEUE, workerQueue);
            
            while (true)
            {
                switch (worker.receiveStartingMessage()) {
                    case ADDITION:
                        requestCounter = 0;
                        worker.sendConfirmation();
                        worker.matrixAddition();
                        waitingTimeInMs = 100;
                        break;
                    case MULTIPLICATION:
                        requestCounter = 0;
                        worker.sendConfirmation();
                        worker.matrixMultiplication();
                        waitingTimeInMs = 100;
                        break;
                    case SUSPENSION:
                        waitingTimeInMs = 3000;
                    case END_OF_PROGRAM:
                        worker.removeWorkerQueue();
                        return;
                }
                Thread.sleep(waitingTimeInMs);
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
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    
    public static void incRequest()
    {
        requestCounter++;
    }
    
    public static Integer getRequests()
    {
        return requestCounter;
    }
}
