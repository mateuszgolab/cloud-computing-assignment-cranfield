package uk.ac.cranfield.cloudcomputing.assignment.test;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

public class MatrixTest
{
    
    @Test
    public void rowsAdditionTest()
    {
        Logger.getLogger("com.amazonaws.request").setLevel(Level.OFF);
        
        
        // Matrix m1 = new Matrix(10);
        // m1.generateRandomValues(512);
        // Matrix m2 = new Matrix(10);
        // m2.generateRandomValues(512);
        //
        // Integer[] sum = new Integer[10];
        // Integer[] rowA = m1.getRow(1);
        // Integer[] rowB = m2.getRow(1);
        //
        // for (int i = 0; i < 10; i++)
        // {
        // sum[i] = rowA[i] + rowB[i];
        // }
        //
        // Worker w = new Worker();
        // Master m = new Master(1);
        //
        //
        // MatrixDoubleDataChunk chunk = new MatrixDoubleDataChunk(m1.getRow(1), m2.getRow(1), 0, 10);
        // Message me = new Message();
        // me.setBody(chunk.toString());
        //
        // Integer[] ints = m.getRow(w.processDataAddition(me));
        //
        //
        // for (int i = 0; i < 10; i++)
        // {
        // if (!sum[i].equals(ints[i]))
        // Assert.fail();
        //
        // }

    }
}
