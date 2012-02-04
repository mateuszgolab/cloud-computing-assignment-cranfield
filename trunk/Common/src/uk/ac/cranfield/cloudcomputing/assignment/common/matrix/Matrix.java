package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class Matrix
{
    
    
    private Integer[][] matrix;
    private int size;
    
    public Matrix(Integer size)
    {
        this.size = size;
        matrix = new Integer[size][size];
    }
    
    public void generateRandomValues(Integer key)
    {
        Random random = new Random(System.currentTimeMillis());
        
        for (int i = 0; i < size; i++)
        {
            for (int j = 0; j < size; j++)
            {
                matrix[i][j] = 1 + random.nextInt(key - 1);
            }
        }
    }
    
    public void setRows(List<Integer[]> rows, Integer rowIndex)
    {
        int endIndex = rowIndex + rows.size();
        for (int i = rowIndex; i < endIndex; i++)
        {
            matrix[i] = rows.get(i - rowIndex);
        }
    }
    
    public void print()
    {
        for (int i = 0; i < size; i++)
        {
            for (int j = 0; j < size; j++)
            {
                System.out.print(matrix[i][j] + " ");
            }
            System.out.println("");
        }
        System.out.println("================================================================================");
    }
    
    public Matrix add(Matrix m)
    {
        long time = System.currentTimeMillis();
        
        if (m.getSize() != size)
            return null;
        
        Matrix result = new Matrix(size);
        
        for (int i = 0; i < size; i++)
        {
            for (int j = 0; j < size; j++)
            {
                result.setValue(i, j, matrix[i][j] + m.getValue(i, j));
            }
        }
        
        // System.out.println("Sequential " + size + " x " + size + " matrix addition time elapsed : "
        // + Integer.toString((int) (System.currentTimeMillis() - time)) + " ms");
        //
        return result;
    }
    
    public Matrix multiply(Matrix m)
    {
        long time = System.currentTimeMillis();
        
        if (m.getSize() != size)
            return null;
        
        Matrix result = new Matrix(size);
        
        for (int i = 0; i < size; i++)
        {
            for (int j = 0; j < size; j++)
            {
                int tmp = 0;
                for (int k = 0; k < size; k++)
                {
                    tmp += matrix[i][k] * m.getValue(k, j);
                }
                result.setValue(i, j, tmp);
            }
        }
        
        // System.out.println("Sequential " + size + " x " + size + " matrix multiplication time elapsed : "
        // + Integer.toString((int) (System.currentTimeMillis() - time)) + " ms");
        
        return result;
    }
    
    public Integer getSize()
    {
        return size;
    }
    
    public Integer getValue(int i, int j)
    {
        return matrix[i][j];
    }
    
    public void setValue(int i, int j, Integer value)
    {
        matrix[i][j] = value;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof Matrix))
            return false;
        Matrix m = (Matrix) obj;
        if (size != m.getSize())
            return false;
        
        for (int i = 0; i < size; i++)
        {
            for (int j = 0; j < size; j++)
            {
                if (!matrix[i][j].equals(m.getValue(i, j)))
                    return false;
            }
        }
        
        return true;
    }
    
    public List<Integer[]> getRows(int startIndex, int n)
    {
        List<Integer[]> list = new ArrayList<Integer[]>();
        
        for (int i = 0; i < n; i++)
        {
            list.add(getRow(startIndex + i));
        }
        
        return list;
        
    }
    
    public Integer[] getRow(int rowIndex)
    {
        Integer[] row = new Integer[size];
        
        for (int i = 0; i < size; i++)
        {
            row[i] = matrix[rowIndex][i];
        }
        
        return row;
    }
    
    public Integer[] getColumn(Integer columnIndex)
    {
        Integer[] column = new Integer[size];
        
        for (int i = 0; i < size; i++)
        {
            column[i] = matrix[i][columnIndex];
        }
        
        return column;
    }
    
    /**
     * size - matrix row/column size
     * rows - number of rows in one chunk
     * if matrix is too big to divide to parts, new parts value are calculated
     * @param parts number of matrix parts
     * @return
     */
    public List<MatrixDataChunk> decompose(int parts)
    {
        int rows = 0;
        List<MatrixDataChunk> result = new ArrayList<MatrixDataChunk>();
        
        rows = size / parts;
        
        if (parts % rows != 0)
            rows++;
        
        if (rows * size > MatrixDataChunk.SIZE_LIMIT)
        {
            rows = MatrixDataChunk.SIZE_LIMIT / size;
            parts = size / rows;
            if (size % rows != 0)
                parts++;
            
        }
        
        rows = size / parts;
        
        if (size % parts == 0)
        {
            for (int i = 0; i < size; i += rows)
            {
                MatrixDataChunk chunk = new MatrixDataChunk(rows, i, size, getRows(i, rows));
                result.add(chunk);
            }
        }
        else
        {
            int spareRows = size - rows * parts;
            int extendedRows = rows + 1;
            int i = 0;
            
            for (; i < spareRows * (extendedRows); i += extendedRows)
            {
                MatrixDataChunk chunk = new MatrixDataChunk(extendedRows, i, size, getRows(i, extendedRows));
                result.add(chunk);
            }
            
            for (; i < size; i += rows)
            {
                MatrixDataChunk chunk = new MatrixDataChunk(rows, i, size, getRows(i, rows));
                result.add(chunk);
            }
        }
        
        return result;
    }
    
    /**
     * size - matrix column size
     * doubleSize - matrix row size
     * rows - number of rows in one chunk
     * if matrix is too big to divide to parts, new parts value are calculated
     * @param parts number of matrix parts
     * @return
     */
    public List<MatrixDoubleDataChunk> decompose(int parts, Matrix matrixB)
    {
        int rows = 0;
        int doubleSize = 2 * size;
        List<MatrixDoubleDataChunk> result = new ArrayList<MatrixDoubleDataChunk>();
        
        rows = size / parts;
        
        if (rows * doubleSize > MatrixDataChunk.SIZE_LIMIT)
        {
            rows = MatrixDataChunk.SIZE_LIMIT / doubleSize;
            parts = size / rows;
            if (MatrixDataChunk.SIZE_LIMIT % size != 0)
                parts++;
        }
        
        
        if (size % parts == 0)
        {
            for (int i = 0; i < size; i += rows)
            {
                MatrixDoubleDataChunk chunk = new MatrixDoubleDataChunk(rows, i, size, getRows(i, rows),
                        matrixB.getRows(i, rows));
                result.add(chunk);
            }
        }
        else
        {
            int spareRows = size - rows * parts;
            int extendedRows = rows + 1;
            int i = 0;
            
            for (; i < spareRows * (extendedRows); i += extendedRows)
            {
                MatrixDoubleDataChunk chunk = new MatrixDoubleDataChunk(extendedRows, i, size,
                        getRows(i, extendedRows), matrixB.getRows(i, extendedRows));
                result.add(chunk);
            }
            
            for (; i < size; i += rows)
            {
                MatrixDoubleDataChunk chunk = new MatrixDoubleDataChunk(rows, i, size, getRows(i, rows),
                        matrixB.getRows(i, rows));
                result.add(chunk);
            }
        }
        
        return result;
    }
}
