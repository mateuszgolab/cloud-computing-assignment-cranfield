package uk.ac.cranfield.cloudcomputing.assignment.master;

import java.util.logging.Level;
import java.util.logging.Logger;

import uk.ac.cranfield.cloudcomputing.assignment.common.Matrix;




public class MainMaster
{
    
    public static final String DATA_QUEUE = "matDataQueue";
    public static final String RESULT_QUEUE = "matResultQueue";
    public static final Integer N = 1000;
    public static final Integer KEY = 1000;

    private static Matrix matrixA;
    private static Matrix matrixB;
    private static Matrix matrixC;
    private static Matrix matrixD;

    
    public static void main(String[] args)
    {
        Logger.getLogger("com.amazonaws.request").setLevel(Level.SEVERE);

        
        matrixA = new Matrix(N);
        matrixA.generateRandomValues(KEY);
        matrixB = new Matrix(N);
        matrixB.generateRandomValues(KEY);
        matrixC = matrixA.add(matrixB);
        matrixD = new Matrix(N);
        
        
        Master m = new Master(matrixA, matrixB, KEY);
        m.connectQueues(DATA_QUEUE, RESULT_QUEUE);
        m.distributeData();
        


    }
    



}
