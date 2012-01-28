package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;


public class MatrixAdditionDataChunk extends MatrixDataChunk
{
    
    private Integer[] rowA;
    private Integer[] rowB;
    
    public MatrixAdditionDataChunk(Integer[] a, Integer[] b, Integer rowIndex, Integer size)
    {
        super(rowIndex, size);
        rowA = a;
        rowB = b;
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
        result += getData(rowA);
        result += getData(rowB);
        
        return result;
        
    }
    
}
