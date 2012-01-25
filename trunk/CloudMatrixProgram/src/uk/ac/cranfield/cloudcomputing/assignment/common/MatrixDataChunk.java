package uk.ac.cranfield.cloudcomputing.assignment.common;


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
}
