package uk.ac.cranfield.cloudcomputing.assignment.worker;

import java.util.logging.Level;
import java.util.logging.Logger;




public class MainWorker
{
    
    public static final String DATA_QUEUE = "matDataQueue";
    public static final String RESULT_QUEUE = "matResultQueue";
    



    public static void main(String[] args)
    {

        Logger.getLogger("com.amazonaws.request").setLevel(Level.SEVERE);

        Worker w = new Worker();
        // Receive data from matrix B at first
        // receive()

        w.connectQueues(DATA_QUEUE, RESULT_QUEUE);
        w.receive();
        
        // encoding();
        
        
    }
    



}
