package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;

import java.util.Arrays;


public class MatrixDataChunk
{
    
    public static final String separator = ",";
    protected Integer rowIndex;
    protected Integer size;
    protected Integer[] data;
    protected String[] results;
    
    
    public MatrixDataChunk(Integer rowIndex, Integer size, Integer data[])
    {
        this.rowIndex = rowIndex;
        this.size = size;
        this.data = data;
    }
    
    public MatrixDataChunk(String string)
    {
        results = string.split(separator);
        
        rowIndex = Integer.parseInt(results[0]);
        size = Integer.parseInt(results[1]);
        data = new Integer[size];
        Integer it = 2;
        
        for (int i = it; i < it + size; i++)
        {
            data[i - it] = Integer.parseInt(results[i]);
        }
    }
    
    /**
     * ******************************************
     * |rowIndex|size|Matrix row|
     * ******************************************
     */
    @Override
    public String toString()
    {
        String result = "";
        
        result += rowIndex.intValue() + separator;
        result += size + separator;
        result += getData(data);
        
        return result;
        
    }
    
    protected String getData(Integer[] data)
    {
        String result = Arrays.toString(data).replace(" ", "");
        return result.substring(1, result.length() - 1);
    }
    
    public Integer getRowIndex()
    {
        return rowIndex;
    }
    
    public Integer[] getMatrixRow()
    {
        return data;
    }
}
