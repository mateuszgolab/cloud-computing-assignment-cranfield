package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MatrixDataChunk
{
    
    public static final int SIZE_LIMIT = 65000;
    public static final String SEPARATOR = ",";
    protected int rowIndex;
    protected int size;
    protected int numberOfRows;
    protected List<Integer[]> data;
    protected String[] results;
    
    
    public MatrixDataChunk()
    {
        data = new ArrayList<Integer[]>();
    }
    
    public MatrixDataChunk(int numberOfRows, int rowIndex, int size, List<Integer[]> data)
    {
        this.numberOfRows = numberOfRows;
        this.rowIndex = rowIndex;
        this.size = size;
        this.data = data;
    }
    
    public MatrixDataChunk(String string)
    {
        this();
        results = string.split(SEPARATOR);
        numberOfRows = Integer.parseInt(results[0]);
        rowIndex = Integer.parseInt(results[1]);
        size = Integer.parseInt(results[2]);
        
        
        for (int j = 0; j < numberOfRows; j++)
        {
            Integer[] row = new Integer[size];
            
            for (int i = 0; i < size; i++)
            {
                row[i] = results[3].charAt(j * size + i) - '0';
            }
            
            data.add(row);
        }
    }
    
    /**
     * ******************************************
     * |numberOfows|rowIndex|size|Matrix rows|
     * ******************************************
     */
    @Override
    public String toString()
    {
        String result = "";
        
        result += numberOfRows + SEPARATOR;
        result += rowIndex + SEPARATOR;
        result += size + SEPARATOR;
        
        for (int i = 0; i < numberOfRows; i++)
            result += getData(data.get(i));
        
        return result;
        
    }
    
    protected String getData(Integer[] data)
    {
        String result = Arrays.toString(data).replace(", ", "");
        return result.substring(1, result.length() - 1);
    }
    
    public int getRowIndex()
    {
        return rowIndex;
    }
    
    public List<Integer[]> getMatrixRows()
    {
        return data;
    }
    
    public int getSize()
    {
        return size;
    }
    
    public int getNumberOfRows()
    {
        return numberOfRows;
    }
    
    public static int getRowLength(Integer[] row)
    {
        return Arrays.toString(row).replace(" ", "").length() - 2;
    }
    
    
}
