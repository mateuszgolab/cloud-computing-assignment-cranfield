package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;


public enum Operation
{
    ADDITION("Add matrixes"), MULTIPLICATION("Multiply matrixes"), END_CALCULATIONS("End"), END_PROGRAM("End program"), RECEPTION(
            "Received");
    
    private String operation;
    
    Operation(String o)
    {
        operation = o;
    }
    
    @Override
    public String toString()
    {
        return operation;
    }
    
}
