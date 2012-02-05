package uk.ac.cranfield.cloudcomputing.assignment;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;

import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Operation;
import uk.ac.cranfield.cloudcomputing.assignment.environment.CloudEnvironment;
import uk.ac.cranfield.cloudcomputing.assignment.master.Master;
import uk.ac.cranfield.cloudcomputing.assignment.master.MatrixDataUploader;
import uk.ac.cranfield.cloudcomputing.assignment.master.MatrixDoubleDataUploader;
import uk.ac.cranfield.cloudcomputing.assignment.view.MainPanel;
import uk.ac.cranfield.cloudcomputing.assignment.view.StatusPanel;


public class Controller implements ActionListener
{
    
    public static final String DATA_QUEUE = "matDataQueue";
    public static final String RESULT_QUEUE = "matResultQueue";
    public static final String IMAGE_NAME = "matWorkerAMI";
    public static final String LINUX_32_AMI = "ami-973b06e3";
    public static final String WORKER_AMI = "ami-53fdc327";
    public static final String WORKER_NAME = "matWorker";
    public static final Integer KEY = 10;
    private MainPanel panel;
    private StatusPanel status;
    private CloudEnvironment env;
    public List<String> workersQueues;
    private Matrix matrixA;
    private Matrix matrixB;
    private Matrix matrixResult;
    private Matrix matrixLocalResult;
    private Master master;
    private int numberOfNodes;
    private int matrixSize;
    private int numberOfDataBlocks;
    
    public Controller(MainPanel panel, StatusPanel status)
    {
        this.panel = panel;
        this.status = status;
        env = new CloudEnvironment();
        
    }
    
    @Override
    public void actionPerformed(ActionEvent e)
    {
        if (e.getSource().equals(panel.getStartAdditionButton()))
        {
            if (panel.wrongValues())
                return;
            
            panel.clear();
            numberOfDataBlocks = panel.getNumberOfDataBlocks();
            matrixSize = panel.getMatrixSize();
            numberOfNodes = panel.getNumberOfNodes();
            
            if (env.getInstancesIds().size() == 0)
                env.createInstances(numberOfNodes, WORKER_NAME, WORKER_AMI);
            
            master = new Master(numberOfNodes, matrixSize, status);
            master.connectToQueues(DATA_QUEUE, RESULT_QUEUE);
            generateMatrixes();
            
            long distTime = distributedMatrixAddition();
            panel.setDistResults("Distributed matrix addition time : " + distTime + " ms");
            long localTime = localMatrixAddition();
            panel.setLocalResults("Local matrix addition time : " + localTime + " ms");
            
            validateResults();
            
            master.receiveMessages(Operation.CONFIRMATION);
            
            
        }
        else if (e.getSource().equals(panel.getStartMultiplicatioButton()))
        {
            if (panel.wrongValues())
                return;
            
            panel.clear();
            numberOfDataBlocks = panel.getNumberOfDataBlocks();
            matrixSize = panel.getMatrixSize();
            numberOfNodes = panel.getNumberOfNodes();
            
            if (env.getInstancesIds().size() == 0)
            {
                env.createInstances(numberOfNodes, WORKER_NAME, WORKER_AMI);
                status.print("Creating instances ...");
            }
            
            master = new Master(numberOfNodes, matrixSize, status);
            master.connectToQueues(DATA_QUEUE, RESULT_QUEUE);
            generateMatrixes();
            
            long distTime = distributedMatrixMultiplication();
            panel.setDistResults("Distributed matrix multiplication time : " + distTime + " ms");
            long localTime = localMatrixMultiplication();
            panel.setLocalResults("Local matrix multiplication time : " + localTime + " ms");
            
            validateResults();
            
            master.receiveMessages(Operation.CONFIRMATION);
            
            
        }
        else if (e.getSource().equals(panel.getExitProgramButton()))
        {
            if (master != null)
                master.sendMessage(Operation.END_PROGRAM);
            env.terminateInstances();
            System.exit(0);
            
        }
    }
    
    private long localMatrixAddition()
    {
        long time = System.currentTimeMillis();
        matrixLocalResult = matrixA.add(matrixB);
        return System.currentTimeMillis() - time;
    }
    
    private long localMatrixMultiplication()
    {
        long time = System.currentTimeMillis();
        matrixLocalResult = matrixA.multiply(matrixB);
        // matrixLocalResult.print();
        return System.currentTimeMillis() - time;
        
    }
    
    private void generateMatrixes()
    {
        status.print("generating A and B matrixes ...");
        matrixA = new Matrix(matrixSize);
        matrixA.generateRandomValues(KEY);
        matrixB = new Matrix(matrixSize);
        matrixB.generateRandomValues(KEY);
        
    }
    
    private long distributedMatrixAddition()
    {
        long time = System.currentTimeMillis();
        master.sendMessage(Operation.ADDITION);
        master.receiveMessages(Operation.CONFIRMATION);
        MatrixDoubleDataUploader doubleUploader = new MatrixDoubleDataUploader(matrixA, matrixB,
                master.getDataQueueURL(), numberOfDataBlocks);
        doubleUploader.send();
        // matrixResult = master.receiveResults();
        return System.currentTimeMillis() - time;
        
    }
    
    private long distributedMatrixMultiplication()
    {
        long time = System.currentTimeMillis();
        
        status.print("distributed multiplication started");
        master.sendMessage(Operation.MULTIPLICATION);
        
        status.print("uploading B matrixes ...");
        MatrixDataUploader uploader = new MatrixDataUploader(matrixB, env.getWorkerQueuesNames(), 1);
        uploader.connectToQueue();
        uploader.send();
        
        master.receiveMessages(Operation.CONFIRMATION);
        master.receiveMessages(Operation.CONFIRMATION);
        uploader.sendMessageToWorkers(Operation.CONFIRMATION);
        
        status.print("uploading A matrix chunks ...");
        uploader = new MatrixDataUploader(matrixA, master.getDataQueueURL(), numberOfDataBlocks);
        uploader.send();
        
        matrixResult = master.receiveResults();
        // matrixResult.print();
        return System.currentTimeMillis() - time;
    }
    
    private void validateResults()
    {
        status.print("Validating results ...");
        
        if (matrixLocalResult.equals(matrixResult))
            panel.setValidationResult("matrixes are equal");
        else
            panel.setValidationResult("matrixes are different");
        
    }
    
}
