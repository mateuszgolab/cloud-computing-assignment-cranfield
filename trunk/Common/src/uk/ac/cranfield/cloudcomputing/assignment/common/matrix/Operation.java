package uk.ac.cranfield.cloudcomputing.assignment.common.matrix;


public enum Operation
{
    ADDITION("Add matrixes"), MULTIPLICATION("Multiply matrixes"), END_OF_CALCULATIONS("End"), END_OF_PROGRAM("End program"), CONFIRMATION(
            "Received"), SUSPENSION("Suspended");
    
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
