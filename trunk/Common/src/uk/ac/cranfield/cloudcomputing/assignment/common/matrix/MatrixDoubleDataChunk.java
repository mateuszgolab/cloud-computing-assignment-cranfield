package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;

import java.util.ArrayList;
import java.util.List;


public class MatrixDoubleDataChunk extends MatrixDataChunk
{
    
    protected List<Integer[]> data2;
    
    public MatrixDoubleDataChunk(int numberOfRows, int rowIndex, int size, List<Integer[]> data, List<Integer[]> data2)
    {
        super(numberOfRows, rowIndex, size, data);
        this.data2 = data2;
        
    }
    
    public MatrixDoubleDataChunk(String string)
    {
        results = string.split(SEPARATOR);
        numberOfRows = Integer.parseInt(results[0]);
        rowIndex = Integer.parseInt(results[1]);
        size = Integer.parseInt(results[2]);
        
        data2 = new ArrayList<Integer[]>();
        
        
        for (int j = 0; j < numberOfRows; j++)
        {
            Integer[] row = new Integer[size];
            Integer[] row2 = new Integer[size];
            int i = 0;
            
            for (; i < size; i++)
            {
                row[i] = results[3].charAt(j * 2 * size + i) - '0';
            }
            
            data.add(row);
            
            for (; i < 2 * size; i++)
            {
                row2[i - size] = results[3].charAt(j * 2 * size + i) - '0';
            }
            
            data2.add(row2);
        }
        
    }
    
    
    /**
     * ****************************************************
     * |numberOfows|rowIndex|size|MatrixA rows MatrixB rows
     * ****************************************************
     */
    @Override
    public String toString()
    {
        String result = "";
        
        result += numberOfRows + SEPARATOR;
        result += rowIndex + SEPARATOR;
        result += size + SEPARATOR;
        
        for (int i = 0; i < numberOfRows; i++)
        {
            result += getData(data.get(i));
            result += getData(data2.get(i));
        }
        
        return result;
        
    }
    
    public List<Integer[]> getMatrixBRows()
    {
        return data2;
    }
    
}
