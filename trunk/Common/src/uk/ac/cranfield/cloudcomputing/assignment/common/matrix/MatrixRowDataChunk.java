package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;


public class MatrixRowDataChunk extends MatrixDataChunk
{
    
    public MatrixRowDataChunk(Integer rowIndex, Integer size, Integer[] row)
    {
        super(rowIndex, size);
        this.row = row;
    }
    
    private Integer row[];
    
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
        result += getData(row);
        
        return result;
        
    }
    
    @Override
    protected String getData(Integer[] data)
    {
        String result = "";
        for (int i = 0; i < data.length; i++)
        {
            result += data[i];
        }
        
        return result;
        
    }
}
