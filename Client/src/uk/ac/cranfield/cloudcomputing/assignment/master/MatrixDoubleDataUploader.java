package uk.ac.cranfield.cloudcomputing.assignment.master;

import java.util.List;

import uk.ac.cranfield.cloudcomputing.assignment.Controller;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixDoubleDataChunk;

import com.amazonaws.services.sqs.model.SendMessageRequest;


public class MatrixDoubleDataUploader extends MatrixDataUploader
{
    
    private Matrix matrix2;
    
    // public MatrixDoubleDataUploader(Matrix matrixA, Matrix matrixB, List<String> queues, int numberOfDataBlocks)
    // {
    // super(matrixA, queues, numberOfDataBlocks);
    // matrix2 = matrixB;
    // }
    
    public MatrixDoubleDataUploader(Matrix matrixA, Matrix matrixB, String queue, int numberOfDataBlocks)
    {
        super(matrixA, queue, numberOfDataBlocks);
        matrix2 = matrixB;
        
        
    }
    
    @Override
    public void send(boolean info)
    {
        List<MatrixDoubleDataChunk> chunks = matrix.decompose(numberOfDataBlocks, matrix2);
        
        for (MatrixDoubleDataChunk chunk : chunks)
        {
            String data = chunk.toString();
            for (String queue : queuesURLs)
            {
                SendMessageRequest smr = new SendMessageRequest(queue, data);
                sqsClient.sendMessage(smr);
                Controller.incRequest();
            }
        }
        
    }
}
