package uk.ac.cranfield.cloudcomputing.assignment.common;



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
        result += rowA.length + separator;
        result += getRowData(rowA);
        result += getRowData(rowB);

        return result;

    }
    
    public String getRowData(Integer[] row)
    {
        String result = "";
        for (int i = 0; i < row.length; i++)
        {
            result += row[i] + separator;
        }
        
        return result;
        
    }


}