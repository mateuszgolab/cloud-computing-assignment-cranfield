package uk.ac.cranfield.cloudcomputing.assignment;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
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
    // public static final String LINUX_32_AMI = "ami-973b06e3";
    public static final String LINUX_32_AMI = "ami-913b05e5";
    public static final Integer KEY = 10;
    public static final Integer SIZE = 100;
    public static List<String> workersQueues;
    
    private static Matrix matrixA;
    private static Matrix matrixB;
    private static Matrix matrixResult;
    private static Matrix matrixLocalResult;
    private static Master master;
    private static AWSCredentials credentials;
    private static Integer numberOfWorkers;
    
    
    /**
     * @param args
     */
    public static void main(String[] args)
    {
        Logger.getLogger("com.amazonaws.request").setLevel(Level.SEVERE);
        
        credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        CloudEnvironment env = new CloudEnvironment(credentials);
        // env.createInstance("matWorker2", LINUX_32_AMI);
        // workersQueues = env.getWorkerQueuesNames();
        workersQueues = new ArrayList<String>();
        workersQueues.add("i-c0f08e89workerQueue");
        // workersQueues.add("i-8c82fbc5workerQueue");
        
        numberOfWorkers = 1;
        master = new Master(numberOfWorkers, SIZE);
        // master.clearQueue(DATA_QUEUE, 100);
        master.connectQueues(DATA_QUEUE, RESULT_QUEUE);
        
        generateMatrixes();
        
        // long distTime = distributedMatrixAddition();
        // long localTime = localMatrixAddition();
        
        long distTime = distributedMatrixMultiplication();
        long localTime = localMatrixMultiplication();
        
        validateResults();
        
        System.out.println("Distributed matrix addition : " + distTime + " ms");
        System.out.println("Local matrix addition : " + localTime + " ms");
        
        // master.endProgram();
        
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
        MatrixDoubleDataUploader doubleUploader = new MatrixDoubleDataUploader(matrixA, matrixB, DATA_QUEUE,
                credentials);
        doubleUploader.connectQueue();
        doubleUploader.start();
        matrixResult = master.receiveResults();
        return System.currentTimeMillis() - time;
        
    }
    
    private static long distributedMatrixMultiplication()
    {
        long time = System.currentTimeMillis();
        MatrixDataUploader uploader = null;
        Integer i = 0;
        
        for (; i < numberOfWorkers; i++)
        {
            uploader = new MatrixDataUploader(matrixB, workersQueues.get(i), credentials);
            uploader.connectQueue();
            uploader.start();
            
            if (master.isMultiplicationNotificationReceived())
                break;
        }
        
        
        uploader = new MatrixDataUploader(matrixA, DATA_QUEUE, credentials);
        uploader.connectQueue();
        uploader.start();
        i++;
        
        for (; i < numberOfWorkers; i++)
        {
            uploader = new MatrixDataUploader(matrixB, workersQueues.get(i), credentials);
            uploader.connectQueue();
            uploader.start();
            
        }
        
        matrixResult = master.receiveResults();
        return System.currentTimeMillis() - time;
    }
    
    private static void validateResults()
    {
        if (matrixLocalResult.equals(matrixResult))
            System.out.println("matrixes are equal");
        else
            System.out.println("matrixes are different");
        
    }
    
    
}
