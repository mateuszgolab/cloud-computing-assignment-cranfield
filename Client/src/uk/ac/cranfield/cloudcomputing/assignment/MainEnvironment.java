package uk.ac.cranfield.cloudcomputing.assignment;

import uk.ac.cranfield.cloudcomputing.assignment.enironment.EnvironmentSetter;





public class MainEnvironment
{
    
    /**
     * @param args
     */
    public static void main(String[] args)
    {
        EnvironmentSetter env = new EnvironmentSetter();
        // env.createKeyPair();
        // env.createSecurityGroup();
        env.createInstance("matWorker");
        




    }
}
