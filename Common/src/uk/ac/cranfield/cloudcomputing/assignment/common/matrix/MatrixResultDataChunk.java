package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;

import java.util.Arrays;
import java.util.List;


public class MatrixResultDataChunk extends MatrixDataChunk
{
    
    public MatrixResultDataChunk(int numberOfRows, int rowIndex, int size, List<Integer[]> data)
    {
        super(numberOfRows, rowIndex, size, data);
    }
    
    public MatrixResultDataChunk(String string)
    {
        super();
        results = string.split(SEPARATOR);
        numberOfRows = Integer.parseInt(results[0]);
        rowIndex = Integer.parseInt(results[1]);
        size = Integer.parseInt(results[2]);
        
        for (int j = 0; j < numberOfRows; j++)
        {
            Integer[] row = new Integer[size];
            
            for (int i = 0; i < size; i++)
            {
                row[i] = Integer.parseInt(results[3 + j * size + i]);
            }
            
            data.add(row);
        }
    }
    
    @Override
    protected String getData(Integer[] data)
    {
        String result = Arrays.toString(data).replace(" ", "");
        result = result.substring(1, result.length() - 1);
        return result + SEPARATOR;
    }
    
}
