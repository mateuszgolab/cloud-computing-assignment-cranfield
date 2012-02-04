package uk.ac.cranfield.cloudcomputing.assignment.worker;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.AmazonServiceException;


public class Main
{
    
    public static final String DATA_QUEUE = "matDataQueue";
    public static final String RESULT_QUEUE = "matResultQueue";
    private static String workerQueue;


    public static void main(String[] args)
    {
        Logger.getLogger("com.amazonaws.request").setLevel(Level.SEVERE);
        
        if (args == null || args.length < 1)
            return;
        workerQueue = args[0] + "_matWorkerQueue";

        // workerQueue = "i-6c4a0c_matWorkerQueue";
        
        try
        {
            
            Worker worker = new Worker();
            worker.connectToQueues(DATA_QUEUE, RESULT_QUEUE, workerQueue);
            
            while (true)
            {
                switch (worker.receiveStartingMessage()) {
                    case ADDITION:
                        worker.sendConfirmation();
                        worker.matrixAddition();
                        break;
                    case MULTIPLICATION:
                        worker.sendConfirmation();
                        worker.matrixMultiplication();
                        break;
                    case END_PROGRAM:
                        // worker.removeWorkerQueue();
                        return;
                }
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
    }
}