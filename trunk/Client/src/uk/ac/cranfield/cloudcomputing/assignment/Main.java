package uk.ac.cranfield.cloudcomputing.assignment;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.UIManager.LookAndFeelInfo;

import uk.ac.cranfield.cloudcomputing.assignment.view.MainFrame;


public class Main
{
    
    
    public static void main(String[] args) throws Exception
    {
        Logger.getLogger("com.amazonaws.request").setLevel(Level.SEVERE);
        
        SwingUtilities.invokeAndWait(new Runnable()
        {
            
            @Override
            public void run()
            {
                try
                {
                    
                    for (LookAndFeelInfo info : UIManager.getInstalledLookAndFeels())
                    {
                        if ("Nimbus".equals(info.getName()))
                        {
                            UIManager.setLookAndFeel(info.getClassName());
                            return;
                        }
                    }
                    
                    
                    JOptionPane
                            .showMessageDialog(
                                    null,
                                    "Please update your Java environment to 1.6 version update 10 or higher, to get better visual effect of the application",
                                    "Java update recommended", JOptionPane.INFORMATION_MESSAGE);
                }
                catch (Exception e)
                {
                    JOptionPane.showMessageDialog(null, "Java Look and feel problem occured", "Error",
                            JOptionPane.ERROR_MESSAGE);
                    
                }
                finally
                {
                    new MainFrame();
                }
            }
        });
        
        
    }
    
    // /**
    // * @param args
    // */
    // public static void main(String[] args)
    // {
    // Logger.getLogger("com.amazonaws.request").setLevel(Level.SEVERE);
    //
    // credentials = new BasicAWSCredentials(AWSCredentialsBean.getAccessKeyId(),
    // AWSCredentialsBean.getSecretAccessKey());
    // CloudEnvironment env = new CloudEnvironment(credentials);
    // // env.createImage("i-625f192b", IMAGE_NAME);
    // env.createInstances(NUMBER_OF_WORKERS, "matWorker", WORKER_AMI);
    // workersQueues = env.getWorkerQueuesNames();
    //
    //
    // // workersQueues = new ArrayList<String>();
    // // workersQueues.add("i-58723411_matWorkerQueue");
    // // workersQueues.add("i-5a723413_matWorkerQueue");
    // //
    // // workersQueues.add("i-5c723415_matWorkerQueue");
    // // workersQueues.add("i-5e723417_matWorkerQueue");
    // //
    // // workersQueues.add("i-50723419_matWorkerQueue");
    // // workersQueues.add("i-5272341b_matWorkerQueue");
    // //
    // // workersQueues.add("i-5472341d_matWorkerQueue");
    // // workersQueues.add("i-5672341f_matWorkerQueue");
    // //
    //
    // master = new Master(NUMBER_OF_WORKERS, SIZE, credentials);
    // master.connectToQueues(DATA_QUEUE, RESULT_QUEUE);
    //
    // generateMatrixes();
    //
    // // long distTime = distributedMatrixAddition();
    // // long localTime = localMatrixAddition();
    //
    // long distTime = distributedMatrixMultiplication();
    // System.out.println("Distributed matrix multiplication time : " + distTime + " ms");
    // long localTime = localMatrixMultiplication();
    // System.out.println("Local matrix multiplication time : " + localTime + " ms");
    //
    // validateResults();
    //
    // master.receiveMessages(Operation.CONFIRMATION);
    //
    //
    // master.sendMessage(Operation.END_PROGRAM);
    // env.terminateInstances();
    // //
    //
    // }
    
    
}
