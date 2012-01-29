package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;


public class MatrixDoubleDataChunk extends MatrixDataChunk
{
    
    private Integer[] data2;
    
    public MatrixDoubleDataChunk(Integer rowIndex, Integer size, Integer[] data, Integer[] data2)
    {
        super(rowIndex, size, data);
        this.data2 = data2;
        
    }
    
    public MatrixDoubleDataChunk(String string)
    {
        super(string);
        
        data2 = new Integer[size];
        
        Integer it = 2 + size;
        
        for (int i = it; i < it + size; i++)
        {
            data2[i - it] = Integer.parseInt(results[i]);
        }
    }
    
    
    /**
     * ******************************************
     * |rowIndex|size|Matrix A row|Matrix B row|
     * ******************************************
     */
    @Override
    public String toString()
    {
        String result = "";
        
        result += rowIndex.intValue() + separator;
        result += size + separator;
        result += getData(data);
        result += separator;
        result += getData(data2);
        
        return result;
        
    }
    
    public Integer[] getMatrixBRow()
    {
        return data2;
    }
    
}
