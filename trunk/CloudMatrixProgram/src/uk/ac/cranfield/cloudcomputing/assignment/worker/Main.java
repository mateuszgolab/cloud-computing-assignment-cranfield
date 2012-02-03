package uk.ac.cranfield.cloudcomputing.assignment.worker;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.AmazonServiceException;


public class Main
{
    
    public static final String ADD_DATA_QUEUE = "matDataQueue";
    public static final String MULTIPLY_DATA_QUEUE = "matDataQueue";
    public static final String RESULT_QUEUE = "matResultQueue";
    private static String workerQueue;


    public static void main(String[] args)
    {
        Logger.getLogger("com.amazonaws.request").setLevel(Level.SEVERE);
        
        // if (args == null || args.length < 1)
        // return;
        // workerQueue = args[0] + "workerQueue";

        workerQueue = "i-c0f08e89workerQueue";
        
        try
        {
            
            Worker worker = new Worker(MULTIPLY_DATA_QUEUE, RESULT_QUEUE, workerQueue);
            worker.connectQueues();
            
            while (true)
            {
                switch (worker.receiveStartingMessage()) {
                    case ADDITION:
                        worker.matrixAddition();
                        break;
                    case MULTIPLICATION:
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
