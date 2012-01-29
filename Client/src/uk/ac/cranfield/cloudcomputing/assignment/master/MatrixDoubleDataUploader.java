package uk.ac.cranfield.cloudcomputing.assignment.master;

import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.MatrixDoubleDataChunk;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class MatrixDoubleDataUploader extends MatrixDataUploader
{
    
    private Matrix matrix2;
    
    public MatrixDoubleDataUploader(Matrix matrixA, Matrix matrixB, String queueName, AWSCredentials credentials)
    {
        super(matrixA, queueName, credentials);
        matrix2 = matrixB;
    }
    
    @Override
    public void run()
    {
        for (int i = 0; i < matrix.getSize(); i++)
        {
            MatrixDoubleDataChunk chunk = new MatrixDoubleDataChunk(i, matrix.getSize(), matrix.getRow(i),
                    matrix2.getRow(i));
            SendMessageRequest smr = new SendMessageRequest(queueURL, chunk.toString());
            sqsClient.sendMessage(smr);
        }
    }
    
}
