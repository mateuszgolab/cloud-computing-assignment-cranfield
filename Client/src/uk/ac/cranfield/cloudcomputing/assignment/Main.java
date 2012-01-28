package uk.ac.cranfield.cloudcomputing.assignment;

import java.util.ArrayList;
import java.util.List;

import uk.ac.cranfield.cloudcomputing.assignment.enironment.CloudEnvironment;
import uk.ac.cranfield.cloudcomputing.assignment.matrix.MatrixUploaderThread;


public class Main
{
    
    public static final String LINUX_32_AMI = "ami-973b06e3";
    public static final Integer SIZE = 100;
    public static final Integer KEY = 10;
    public static final String MASTER_QUEUE = "matDataQueue";
    public static List<String> workersQueue;
    
    static
    {
        workersQueue = new ArrayList<String>();
        workersQueue.add("matWorkerDataUploadQ1");
        workersQueue.add("matWorkerDataUploadQ2");
        workersQueue.add("matWorkerDataUploadQ3");
        workersQueue.add("matWorkerDataUploadQ4");
        workersQueue.add("matWorkerDataUploadQ5");
        workersQueue.add("matWorkerDataUploadQ6");
        workersQueue.add("matWorkerDataUploadQ7");
        workersQueue.add("matWorkerDataUploadQ8");
    }
    
    /**
     * @param args
     */
    public static void main(String[] args)
    {
        CloudEnvironment env = new CloudEnvironment();
        // env.createKeyPair();
        // env.createSecurityGroup();
        env.createInstance("matWorker", LINUX_32_AMI);
        
        MatrixUploaderThread master = new MatrixUploaderThread(SIZE, KEY, MASTER_QUEUE);
        master.connectQueue();
        master.start();
        
        MatrixUploaderThread worker = new MatrixUploaderThread(SIZE, KEY, workersQueue.get(0));
        worker.connectQueue();
        worker.start();
        
    }
}
