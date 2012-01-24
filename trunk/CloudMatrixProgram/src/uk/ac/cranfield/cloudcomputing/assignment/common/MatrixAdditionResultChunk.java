package uk.ac.cranfield.cloudcomputing.assignment.common;




public class MatrixAdditionResultChunk
{
    
    private Integer rowIndex;
    private Integer key;
    private Integer[] rowA;
    private Integer[] rowB;
    
    public MatrixAdditionResultChunk(Integer[] a, Integer[] b, Integer row, Integer k)
    {
        rowIndex = row;
        key = k;
        rowA = a;
        rowB = b;
    }
    
    
    /**
     * ******************************************
     * |key|rowIndex|Matrix A row|0|Matrix B row|
     * ******************************************
     */
    @Override
    public String toString()
    {
        String result = "";
        
        result += (char) key.intValue();
        result += (char) rowIndex.intValue();
        result += (char) rowA.length;
        result += getCompressedData(rowA);
        result += getCompressedData(rowB);
        
        return result;
        
    }
    
    private String getCompressedData(Integer[] row)
    {
        String result = "";
        for (int i = 0; i < row.length; i++)
        {
            int value = row[i] + key;
            result += (char) value;
        }
        
        return result;
        
        
    }
    
    

    



}
