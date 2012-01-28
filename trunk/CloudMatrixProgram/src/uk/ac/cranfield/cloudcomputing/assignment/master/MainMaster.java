package uk.ac.cranfield.cloudcomputing.assignment.master;

import java.util.logging.Level;
import java.util.logging.Logger;

import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;





public class MainMaster
{
    
    public static final String DATA_QUEUE = "matDataQueue";
    public static final String RESULT_QUEUE = "matResultQueue";
    public static final Integer N = 3000;
    public static final Integer KEY = 10;

    private static Matrix matrixA;
    private static Matrix matrixB;
    private static Matrix matrixResult;

    
    public static void main(String[] args)
    {
        Logger.getLogger("com.amazonaws.request").setLevel(Level.SEVERE);

        
        // Receive data from matrix A at first
        // receive();

        matrixA = new Matrix(N);
        matrixA.generateRandomValues(KEY);
        matrixB = new Matrix(N);
        matrixB.generateRandomValues(KEY);

        
        // Master m = new Master(matrixA, matrixB, KEY);
        // m.connectQueues(DATA_QUEUE, RESULT_QUEUE);
        // m.distributeData();
        
        // matrixResult = matrixA.add(matrixB);
        matrixResult = matrixA.multiply(matrixB);


    }
    



}
