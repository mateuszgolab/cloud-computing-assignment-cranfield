package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class representing matrix data chunk
 * Used for matrix multiplication
 * @author Mateusz Golab
 * @version 1.0
 */
public class MatrixDataChunk
{
    
    public static final int SIZE_LIMIT = 65000;
    public static final String SEPARATOR = ",";
    protected int rowIndex;
    protected int size;
    protected int numberOfRows;
    protected List<Integer[]> data;
    protected String[] results;
    
    /**
     * default constructor
     */
    public MatrixDataChunk()
    {
        data = new ArrayList<Integer[]>();
    }
    
    /**
     * Constructs matrix data chunk
     * @param numberOfRows number of rows in a chunk
     * @param rowIndex index of first row
     * @param size matrix size
     * @param data matrix chunk data
     */
    public MatrixDataChunk(int numberOfRows, int rowIndex, int size, List<Integer[]> data)
    {
        this.numberOfRows = numberOfRows;
        this.rowIndex = rowIndex;
        this.size = size;
        this.data = data;
    }
    
    /**
     * This constuctor is used to create a chunk from received message
     * @param string body of received message
     */
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
     * Returns chunk data in specified format
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
    
    /**
     * Returns data for message
     * @param data given data
     * @return
     */
    protected String getData(Integer[] data)
    {
        String result = Arrays.toString(data).replace(", ", "");
        return result.substring(1, result.length() - 1);
    }
    
    /**
     * @return index of the first row
     */
    public int getRowIndex()
    {
        return rowIndex;
    }
    
    /**
     * @return matrix rows data
     */
    public List<Integer[]> getMatrixRows()
    {
        return data;
    }
    
    
    /**
     * @return matrix size
     */
    public int getSize()
    {
        return size;
    }
    
    /**
     * -@return number of rows
     */
    public int getNumberOfRows()
    {
        return numberOfRows;
    }
    
    /**
     * @param row returns number of digits in given data row
     * @return
     */
    public static int getRowLength(Integer[] row)
    {
        return Arrays.toString(row).replace(" ", "").length() - 2;
    }
    
    
}
