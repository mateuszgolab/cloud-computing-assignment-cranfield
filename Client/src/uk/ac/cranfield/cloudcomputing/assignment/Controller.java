package uk.ac.cranfield.cloudcomputing.assignment;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import uk.ac.cranfield.cloudcomputing.assignment.common.QueueType;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Matrix;
import uk.ac.cranfield.cloudcomputing.assignment.common.matrix.Operation;
import uk.ac.cranfield.cloudcomputing.assignment.environment.CloudEnvironment;
import uk.ac.cranfield.cloudcomputing.assignment.master.Master;
import uk.ac.cranfield.cloudcomputing.assignment.master.MatrixDataUploader;
import uk.ac.cranfield.cloudcomputing.assignment.master.MatrixDoubleDataUploader;
import uk.ac.cranfield.cloudcomputing.assignment.master.MatrixOperationExecutor;
import uk.ac.cranfield.cloudcomputing.assignment.view.MainPanel;
import uk.ac.cranfield.cloudcomputing.assignment.view.StatusPanel;


public class Controller implements ActionListener
{
    
    public static final String DATA_QUEUE = "matDataQueue";
    public static final String RESULT_QUEUE = "matResultQueue";
    public static final String MESSAGE_QUEUE = "matMessageQueue";
    public static final String IMAGE_NAME = "matWorkerAMI";
    public static final String LINUX_32_AMI = "ami-973b06e3";
    // public static final String WORKER_AMI = "ami-53fdc327";
    // public static final String WORKER_AMI = "ami-2deed059"; << AMI5
    public static final String WORKER_AMI = "ami-bfe9d7cb";
    public static final String WORKER_NAME = "matWorker";
    public static final Integer KEY = 10;
    private MainPanel panel;
    private StatusPanel status;
    private CloudEnvironment env;
    public List<String> workersQueuesURLs;
    private Matrix matrixA;
    private Matrix matrixB;
    private Matrix matrixResult;
    private Matrix matrixLocalResult;
    private Master master;
    private int numberOfNodes;
    private int matrixSize;
    private int numberOfDataBlocks;
    private static Integer requestCounter;
    
    public Controller(MainPanel panel, StatusPanel status)
    {
        this.panel = panel;
        this.status = status;
        env = new CloudEnvironment();
        requestCounter = 0;
    }
    
    @Override
    public void actionPerformed(ActionEvent e)
    {
        if (e.getSource().equals(panel.getStartAdditionButton()))
        {
            if (panel.wrongValues())
                return;
            
            panel.reset();
            status.reset();
            numberOfDataBlocks = panel.getNumberOfDataBlocks();
            matrixSize = panel.getMatrixSize();
            numberOfNodes = panel.getNumberOfNodes();
            requestCounter = 0;
            
            // if (env.getInstancesIds().size() < numberOfNodes)
            // {
            // env.createInstances(numberOfNodes - env.getInstancesIds().size(), WORKER_NAME, WORKER_AMI);
            // status.print("starting instances ...");
            // }
            // else if (env.getInstancesIds().size() > numberOfNodes)
            // {
            // master.sendMessage(Operation.SUSPENSION, env.getInstancesIds().size() - numberOfNodes);
            // }
            DistributedMatrixAddition distAddition = new DistributedMatrixAddition();
            
            master = new Master(numberOfNodes, matrixSize, status, distAddition);
            master.connectToQueues(DATA_QUEUE, RESULT_QUEUE, MESSAGE_QUEUE);
            generateMatrixes();
            
            distAddition.execute();
            
            
        }
        else if (e.getSource().equals(panel.getStartMultiplicatioButton()))
        {
            if (panel.wrongValues())
                return;
            
            panel.reset();
            status.reset();
            numberOfDataBlocks = panel.getNumberOfDataBlocks();
            matrixSize = panel.getMatrixSize();
            numberOfNodes = panel.getNumberOfNodes();
            requestCounter = 0;
            
            // if (env.getInstancesIds().size() < numberOfNodes)
            // {
            // env.createInstances(numberOfNodes - env.getInstancesIds().size(), WORKER_NAME, WORKER_AMI);
            // status.print("starting instances ...");
            // }
            // else if (env.getInstancesIds().size() > numberOfNodes)
            // {
            // master.sendMessage(Operation.SUSPENSION, env.getInstancesIds().size() - numberOfNodes);
            // }
            
            
            DistributedMatrixMultiplication distMultiplication = new DistributedMatrixMultiplication();
            
            master = new Master(numberOfNodes, matrixSize, status, distMultiplication);
            master.connectToQueues(DATA_QUEUE, RESULT_QUEUE, MESSAGE_QUEUE);
            generateMatrixes();
            
            distMultiplication.execute();
            
        }
        else if (e.getSource().equals(panel.getExitProgramButton()))
        {
            if (master != null)
                master.sendMessage(Operation.END_OF_PROGRAM);
            env.terminateInstances();
            // master.removeWorkerQueues(workersQueuesURLs);
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
        status.print("local matrix multiplication started");
        matrixLocalResult = matrixA.multiply(matrixB);
        status.print("local matrix multiplication finished");
        return System.currentTimeMillis() - time;
        
    }
    
    private void generateMatrixes()
    {
        matrixA = new Matrix(matrixSize);
        matrixA.generateRandomValues(KEY);
        matrixB = new Matrix(matrixSize);
        matrixB.generateRandomValues(KEY);
        
    }
    
    private void validateResults()
    {
        status.print("Validating results ...");
        
        if (matrixLocalResult.equals(matrixResult))
            status.print("matrices are equal");
        else
            status.print("matrices are different");
        
    }
    
    private class DistributedMatrixMultiplication extends MatrixOperationExecutor
    {
        
        @Override
        protected Long doInBackground() throws Exception
        {
            
            master = new Master(numberOfNodes, matrixSize, status, DistributedMatrixMultiplication.this);
            master.connectToQueues(DATA_QUEUE, RESULT_QUEUE, MESSAGE_QUEUE);
            
            master.sendMessage(Operation.MULTIPLICATION);
            
            status.print("uploading B matrices...");
            
            
            String[] ss = {"qq2_matWorkerQueue", "qq1_matWorkerQueue"};
            
            // MatrixDataUploader uploader = new MatrixDataUploader(matrixB, env.getWorkerQueuesNames(), MESSAGE_QUEUE,
            // 1);
            MatrixDataUploader uploader = new MatrixDataUploader(matrixB, Arrays.asList(ss), MESSAGE_QUEUE, 1);
            uploader.connectToQueue();
            uploader.send();
            status.print("B matrices uploaded");
            
            // master.receiveMessages(Operation.CONFIRMATION);
            master.receiveMessages(Operation.CONFIRMATION, QueueType.RESULT);
            
            long time = System.currentTimeMillis();
            status.print("distributed matrix multiplication started");
            
            // uploader.sendMessageToWorkers(Operation.CONFIRMATION);
            
            status.print("decomposing and sending matrix A ...");
            uploader = new MatrixDataUploader(matrixA, master.getDataQueueURL(), numberOfDataBlocks);
            uploader.send();
            workersQueuesURLs = uploader.getWorkerQueuesURLs();
            status.print("matrix A chunks uploaded");
            
            status.print("receiving results from the cloud...");
            matrixResult = master.receiveResults();
            // matrixResult.print();
            status.print("distributed matrix multiplication finished");
            return System.currentTimeMillis() - time;
            
        }
        
        @Override
        protected void process(List<Integer> val)
        {
            for (Integer i : val)
                status.updateProgress(i);
        }
        
        @Override
        public void done()
        {
            try
            {
                panel.setDistResults("Distributed matrix multiplication time : " + get() + " ms");
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (ExecutionException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            new LocalMatrixMultiplication().execute();
        }
        
        
    }
    
    private class LocalMatrixMultiplication extends MatrixOperationExecutor
    {
        
        @Override
        protected Long doInBackground() throws Exception
        {
            return localMatrixMultiplication();
        }
        
        @Override
        public void done()
        {
            try
            {
                panel.setLocalResults("Local matrix multiplication time : " + get() + " ms");
                requestCounter += master.receiveEndingMessages(QueueType.MESSAGE);
                panel.setCost(getCost());
                validateResults();
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (ExecutionException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            
        }
        
        @Override
        protected void process(List<Integer> val)
        {
            // TODO Auto-generated method stub
            
        }
    }
    
    private class DistributedMatrixAddition extends MatrixOperationExecutor
    {
        
        
        @Override
        protected Long doInBackground() throws Exception
        {
            
            // master = new Master(numberOfNodes, matrixSize, status, DistributedMatrixAddition.this);
            // master.connectToQueues(DATA_QUEUE, RESULT_QUEUE, MESSAGE_QUEUE);
            
            master.sendMessage(Operation.ADDITION);
            master.receiveMessages(Operation.CONFIRMATION, QueueType.RESULT);
            long time = System.currentTimeMillis();
            
            status.print("decomposing and sending matrix A and B ...");
            MatrixDoubleDataUploader doubleUploader = new MatrixDoubleDataUploader(matrixA, matrixB,
                    master.getDataQueueURL(), numberOfDataBlocks);
            doubleUploader.send();
            status.print("matrix A and B chunks uploaded");
            
            status.print("receiving results from the cloud...");
            matrixResult = master.receiveResults();
            
            return System.currentTimeMillis() - time;
        }
        
        @Override
        protected void process(List<Integer> val)
        {
            for (Integer i : val)
                status.updateProgress(i);
        }
        
        @Override
        public void done()
        {
            try
            {
                requestCounter += master.receiveEndingMessages(QueueType.MESSAGE);
                
                panel.setDistResults("Distributed matrix addition time : " + get() + " ms");
                long time = localMatrixAddition();
                panel.setLocalResults("Local matrix addition time : " + time + " ms");
                panel.setCost(getCost());
                validateResults();
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (ExecutionException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
        }
    }
    
    public static Integer getCost()
    {
        return requestCounter / 1000;
    }
    
    public static void incRequest()
    {
        requestCounter++;
    }
}
