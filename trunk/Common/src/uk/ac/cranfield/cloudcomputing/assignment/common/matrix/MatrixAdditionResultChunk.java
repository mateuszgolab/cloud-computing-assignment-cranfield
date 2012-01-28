package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;




public class MatrixAdditionResultChunk extends MatrixDataChunk
{
    
    private Integer[] row;
    
    public MatrixAdditionResultChunk(Integer[] row, Integer rowIndex, Integer size)
    {
        super(rowIndex, size);
        this.row = row;

    }
    
    
    /**
     * ******************************************
     * |rowIndex|size|Matrix A + B row|
     * ******************************************
     */
    @Override
    public String toString()
    {
        String result = "";
        
        result += rowIndex.intValue() + separator;
        result += size.intValue() + separator;
        result += getRowData();

        return result;

    }
    
    public String getRowData()
    {
        String result = "";
        for (int i = 0; i < row.length; i++)
        {
            result += row[i] + separator;
        }
        
        return result;
        
        

    }
    
    

    



}
