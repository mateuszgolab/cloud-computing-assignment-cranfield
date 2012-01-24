package uk.ac.cranfield.cloudcomputing.assignment.common;

import java.util.Random;


public class Matrix
{
    private Integer[][] matrix;
    private Integer size;
    
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
                matrix[i][j] = random.nextInt(key);
            }
        }
        
    }
    
    public void setRow(Integer rowIndex, Integer[] row)
    {
        if (row.length < size)
            return;

        for (int i = 0; i < size; i++)
        {
            matrix[rowIndex][i] = row[i];
        }
    }

    private void print()
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
        if(m.getSize() != size) return null;
        
        Matrix result = new Matrix(size);
        
        for (int i = 0; i < size; i++)
        {
            for (int j = 0; j < size; j++)
            {
                result.setValue(i, j, matrix[i][j] + m.getValue(i, j));
            }
        }
        
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
        if(!(obj instanceof Matrix)) return false;
        Matrix m = (Matrix) obj;
        if(size != m.getSize()) return false;
        
        for(int i = 0; i < size; i++)
        {
            for(int j = 0; j < size; j++)
            {
                if (matrix[i][j] != m.getValue(i, j))
                    return false;
            }
        }
       
        return true;
    }
    
    public Integer[] getRow(Integer rowIndex)
    {
        Integer[] row = new Integer[size];

        for (int i = 0; i < size; i++)
        {
            row[i] = matrix[rowIndex][i];
        }
        
        return row;
    }
}
