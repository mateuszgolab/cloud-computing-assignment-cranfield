package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;


public class MatrixMultiplicationDataChunk extends MatrixDataChunk
{
    
    private Integer columnIndex;
    private Integer[] row;
    private Integer[] column;
    
    public MatrixMultiplicationDataChunk(Integer[] row, Integer[] column, Integer rowIndex, Integer columnIndex,
            Integer size)
    {
        super(rowIndex, size);
        this.columnIndex = columnIndex;
        this.row = row;
        this.column = column;
    }
    
    /**
     * ******************************************
     * |rowIndex|columnIndex|size|Matrix A row|Matrix B column|
     * ******************************************
     */
    @Override
    public String toString()
    {
        String result = "";
        
        result += rowIndex.intValue() + separator;
        result += columnIndex.intValue() + separator;
        result += size + separator;
        result += getData(row);
        result += getData(column);
        
        return result;
        
    }
    

    
}
