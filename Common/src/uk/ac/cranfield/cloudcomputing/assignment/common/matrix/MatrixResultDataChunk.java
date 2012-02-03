package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;

import java.util.Arrays;
import java.util.List;


public class MatrixResultDataChunk extends MatrixDataChunk
{
    
    public MatrixResultDataChunk(int numberOfRows, int rowIndex, int size, List<Integer[]> data)
    {
        super(numberOfRows, rowIndex, size, data);
    }
    
    @Override
    protected String getData(Integer[] data)
    {
        String result = Arrays.toString(data).replace(" ", "");
        return result.substring(1, result.length() - 1);
    }
    
}
