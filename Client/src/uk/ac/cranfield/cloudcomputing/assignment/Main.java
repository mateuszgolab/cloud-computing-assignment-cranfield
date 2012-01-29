package uk.ac.cranfield.cloudcomputing.assignment;

import java.util.ArrayList;
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
    // public static final String LINUX_32_AMI = "ami-973b06e3";
    public static final String LINUX_32_AMI = "ami-913b05e5";
    public static final Integer KEY = 10;
    public static final Integer SIZE = 100;
    public static List<String> workersQueues;
    
    private static Matrix matrixA;
    private static Matrix matrixB;
    private static Matrix matrixResult;
    private static Master master;
    private static AWSCredentials credentials;
    private static Integer numberOfWorkers;
    
    
    static
    {
        workersQueues = new ArrayList<String>();
        workersQueues.add("matWorkerDataUploadQ1");
        workersQueues.add("matWorkerDataUploadQ2");
        workersQueues.add("matWorkerDataUploadQ3");
        workersQueues.add("matWorkerDataUploadQ4");
        workersQueues.add("matWorkerDataUploadQ5");
        workersQueues.add("matWorkerDataUploadQ6");
        workersQueues.add("matWorkerDataUploadQ7");
        workersQueues.add("matWorkerDataUploadQ8");
    }
    
    
    /**
     * @param args
     */
    public static void main(String[] args)
    {
        Logger.getLogger("com.amazonaws.request").setLevel(Level.SEVERE);
        
        credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        CloudEnvironment env = new CloudEnvironment(credentials);
        env.createInstance("matWorker", LINUX_32_AMI);
        
        // numberOfWorkers = 1;
        // master = new Master(numberOfWorkers);
        //
        // generateAndDistributeMatrixes(Matrix.Operation.ADDITION);
        //
        // distributedMatrixAddition();
        // localMatrixAddition();
        
    }
    
    private static long localMatrixAddition()
    {
        long time = System.currentTimeMillis();
        matrixResult = matrixA.add(matrixB);
        return System.currentTimeMillis() - time;
    }
    
    private static long localMatrixMultiplication()
    {
        long time = System.currentTimeMillis();
        matrixResult = matrixA.multiply(matrixB);
        return System.currentTimeMillis() - time;
        
    }
    
    private static void generateAndDistributeMatrixes(Operation operation)
    {
        matrixA = new Matrix(SIZE);
        matrixA.generateRandomValues(KEY);
        matrixB = new Matrix(SIZE);
        matrixB.generateRandomValues(KEY);
        
        switch (operation) {
            case ADDITION:
                
                MatrixDoubleDataUploader doubleUploader = new MatrixDoubleDataUploader(matrixA, matrixB, DATA_QUEUE,
                        credentials);
                doubleUploader.connectQueue();
                doubleUploader.start();
                
                break;
            case MULTIPLICATION:
                
                MatrixDataUploader uploader = null;
                uploader = new MatrixDataUploader(matrixA, DATA_QUEUE, credentials);
                uploader.connectQueue();
                uploader.start();
                
                for (int i = 0; i < numberOfWorkers; i++)
                {
                    uploader = new MatrixDataUploader(matrixB, workersQueues.get(i), credentials);
                    uploader.connectQueue();
                    uploader.start();
                }
                break;
        }
    }
    
    
    private static void distributedMatrixAddition()
    {
        
    }
    
    private static void distributedMatrixMultiplication()
    {
        
    }
}
