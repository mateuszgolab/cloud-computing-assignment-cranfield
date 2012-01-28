package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;


public abstract class MatrixDataChunk
{
    
    public static final String separator = "-";
    protected Integer rowIndex;
    protected Integer size;
    
    
    public MatrixDataChunk(Integer rowIndex, Integer size)
    {
        this.rowIndex = rowIndex;
        this.size = size;
    }
    
    @Override
    public abstract String toString();
    
    protected String getData(Integer[] data)
    {
        String result = "";
        for (int i = 0; i < data.length; i++)
        {
            result += data[i] + separator;
        }
        
        return result;
        
    }
}
