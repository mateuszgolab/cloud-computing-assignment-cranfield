package uk.ac.cranfield.cloudcomputing.assignment;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Operation;
import uk.ac.cranfield.cloudcomputing.assignment.environment.CloudEnvironment;
import uk.ac.cranfield.cloudcomputing.assignment.master.Master;
import uk.ac.cranfield.cloudcomputing.assignment.master.MatrixDataUploader;
import uk.ac.cranfield.cloudcomputing.assignment.master.MatrixDoubleDataUploader;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;


public class Main
{
    
    private static final String accessKeyId = "AKIAJ2KOCJHIWA4JVTYQ";
    private static final String secretAccessKey = "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";
    public static final String DATA_QUEUE = "matDataQueue";
    public static final String RESULT_QUEUE = "matResultQueue";
    public static final String IMAGE_NAME = "matWorkerAMI";
    // public static final String LINUX_32_AMI = "ami-973b06e3";
    public static final String LINUX_32_AMI = "ami-53fdc327";
    public static final Integer KEY = 10;
    public static final Integer SIZE = 2000;
    private static final int NUMBER_OF_WORKERS = 8;
    public static final int NUMBER_OF_DATA_BLOCKS = 32;
    public static List<String> workersQueues;
    
    private static Matrix matrixA;
    private static Matrix matrixB;
    private static Matrix matrixResult;
    private static Matrix matrixLocalResult;
    private static Master master;
    private static AWSCredentials credentials;
    
    
    /**
     * @param args
     */
    public static void main(String[] args)
    {
        Logger.getLogger("com.amazonaws.request").setLevel(Level.SEVERE);
        
        credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        CloudEnvironment env = new CloudEnvironment(credentials);
        // env.createImage("i-625f192b", IMAGE_NAME);
        env.createInstances(NUMBER_OF_WORKERS, "matWorker", LINUX_32_AMI);
        workersQueues = env.getWorkerQueuesNames();
        
        
        // workersQueues = new ArrayList<String>();
        // workersQueues.add("i-58723411_matWorkerQueue");
        // workersQueues.add("i-5a723413_matWorkerQueue");
        //
        // workersQueues.add("i-5c723415_matWorkerQueue");
        // workersQueues.add("i-5e723417_matWorkerQueue");
        //
        // workersQueues.add("i-50723419_matWorkerQueue");
        // workersQueues.add("i-5272341b_matWorkerQueue");
        //
        // workersQueues.add("i-5472341d_matWorkerQueue");
        // workersQueues.add("i-5672341f_matWorkerQueue");
        //
        
        master = new Master(NUMBER_OF_WORKERS, SIZE);
        master.connectToQueues(DATA_QUEUE, RESULT_QUEUE);
        
        generateMatrixes();
        
        // long distTime = distributedMatrixAddition();
        // long localTime = localMatrixAddition();
        
        long distTime = distributedMatrixMultiplication();
        System.out.println("Distributed matrix multiplication time : " + distTime + " ms");
        long localTime = localMatrixMultiplication();
        System.out.println("Local matrix multiplication time : " + localTime + " ms");
        
        validateResults();
        
        master.receiveMessages(Operation.CONFIRMATION);
        
        
        master.sendMessage(Operation.END_PROGRAM);
        env.terminateInstances();
        //
        
    }
    
    private static long localMatrixAddition()
    {
        long time = System.currentTimeMillis();
        matrixLocalResult = matrixA.add(matrixB);
        return System.currentTimeMillis() - time;
    }
    
    private static long localMatrixMultiplication()
    {
        long time = System.currentTimeMillis();
        matrixLocalResult = matrixA.multiply(matrixB);
        // matrixLocalResult.print();
        return System.currentTimeMillis() - time;
        
    }
    
    private static void generateMatrixes()
    {
        matrixA = new Matrix(SIZE);
        matrixA.generateRandomValues(KEY);
        matrixB = new Matrix(SIZE);
        matrixB.generateRandomValues(KEY);
        
    }
    
    private static long distributedMatrixAddition()
    {
        long time = System.currentTimeMillis();
        master.sendMessage(Operation.ADDITION);
        master.receiveMessages(Operation.CONFIRMATION);
        MatrixDoubleDataUploader doubleUploader = new MatrixDoubleDataUploader(matrixA, matrixB,
                master.getDataQueueURL(), credentials, NUMBER_OF_DATA_BLOCKS);
        doubleUploader.send();
        // matrixResult = master.receiveResults();
        return System.currentTimeMillis() - time;
        
    }
    
    private static long distributedMatrixMultiplication()
    {
        long time = System.currentTimeMillis();
        
        master.sendMessage(Operation.MULTIPLICATION);
        
        MatrixDataUploader uploader = new MatrixDataUploader(matrixB, workersQueues, credentials, 1);
        uploader.connectToQueue();
        uploader.send();
        
        master.receiveMessages(Operation.CONFIRMATION);
        master.receiveMessages(Operation.CONFIRMATION);
        uploader.sendMessageToWorkers(Operation.CONFIRMATION);
        
        uploader = new MatrixDataUploader(matrixA, master.getDataQueueURL(), credentials, NUMBER_OF_DATA_BLOCKS);
        uploader.send();
        
        matrixResult = master.receiveResults();
        // matrixResult.print();
        return System.currentTimeMillis() - time;
    }
    
    private static void validateResults()
    {
        System.out.println("Validating results ...");
        
        if (matrixLocalResult.equals(matrixResult))
            System.out.println("matrixes are equal");
        else
            System.out.println("matrixes are different");
        
    }
    
    
}
