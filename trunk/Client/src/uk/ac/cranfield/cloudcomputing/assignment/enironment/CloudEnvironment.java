package uk.ac.cranfield.cloudcomputing.assignment.enironment;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairResult;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.KeyPair;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;


public class CloudEnvironment
{
    
    private AmazonEC2 clientEC2;
    private AWSCredentials credentials;
    private List<String> securityGroupIds;
    private KeyPair keyPair;
    
    
    public static final String GROUP = "matGroup";
    public static final String SECURITY_GROUP = "matSecurityGroup";
    public static final String SECURITY_GROUP_DESCRIPTION = "Mat's security group";
    public static final String ENDPOINT_ZONE = "https://eu-west-1.ec2.amazonaws.com";
    public static final String KEY_PAIR_NAME = "matKeyPair";
    public static final String SECURITY_GROUP_ID = "sg-3cd83a4b";
    public static final String PROTOCOL = "tcp";
    public static final Integer SSH_PORT = 22;
    public static final String CRANFIELD_SUBNET = "138.250.0.0/16";
    
    private static final String accessKeyId = "AKIAJ2KOCJHIWA4JVTYQ";
    private static final String secretAccessKey = "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";
    
    public CloudEnvironment()
    {
        credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        clientEC2 = new AmazonEC2Client(credentials);
        clientEC2.setEndpoint(ENDPOINT_ZONE);
    }
    
    public void createSecurityGroup()
    {
        
        try
        {
            securityGroupIds = new ArrayList<String>();
            CreateSecurityGroupRequest securityGroupRequest = new CreateSecurityGroupRequest(SECURITY_GROUP,
                    SECURITY_GROUP_DESCRIPTION);
            CreateSecurityGroupResult result = clientEC2.createSecurityGroup(securityGroupRequest);
            securityGroupIds.add(result.getGroupId());
            
            AuthorizeSecurityGroupIngressRequest r2 = new AuthorizeSecurityGroupIngressRequest();
            r2.setGroupName(SECURITY_GROUP);
            IpPermission permission = new IpPermission();
            permission.setIpProtocol(PROTOCOL);
            permission.setFromPort(SSH_PORT);
            permission.setToPort(SSH_PORT);
            List<String> ipRanges = new ArrayList<String>();
            ipRanges.add(CRANFIELD_SUBNET);
            permission.setIpRanges(ipRanges);
            
            List<IpPermission> permissions = new ArrayList<IpPermission>();
            permissions.add(permission);
            r2.setIpPermissions(permissions);
            
            clientEC2.authorizeSecurityGroupIngress(r2);
            
            
        }
        catch (AmazonServiceException ase)
        {
            System.out.println(ase.getMessage());
        }
    }
    
    public void createKeyPair()
    {
        CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest(KEY_PAIR_NAME);
        CreateKeyPairResult createKeyPairResult = clientEC2.createKeyPair(createKeyPairRequest);
        keyPair = createKeyPairResult.getKeyPair();
        String pem = keyPair.getKeyMaterial();
        PrintWriter out = null;
        
        try
        {
            FileWriter outFile = new FileWriter(KEY_PAIR_NAME + ".pem");
            out = new PrintWriter(outFile);
            out.println(pem);
            
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (out != null)
                out.close();
        }
        
    }
    
    public void createInstances(Integer number, List<String> names, String image)
    {
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
        runInstancesRequest.setInstanceType(InstanceType.T1Micro);
        runInstancesRequest.setImageId(image);
        runInstancesRequest.setKeyName(KEY_PAIR_NAME);
        runInstancesRequest.setMinCount(Integer.valueOf(1));
        runInstancesRequest.setMaxCount(number);
        runInstancesRequest.setAdditionalInfo("programmatically set instance");
        
        // Add the security group to the request.
        ArrayList<String> securityGroupIds = new ArrayList<String>();
        securityGroupIds.add(SECURITY_GROUP_ID);
        
        runInstancesRequest.setSecurityGroupIds(securityGroupIds);
        
        // Launch the instance.
        RunInstancesResult runResult = clientEC2.runInstances(runInstancesRequest);
        
        
        List<Instance> ins = runResult.getReservation().getInstances();
        
        
        if (names != null)
        {
            int i = 0;
            for (Instance instance : ins)
            {
                CreateTagsRequest createTagsRequest = new CreateTagsRequest();
                createTagsRequest.withResources(instance.getInstanceId()).withTags(new Tag("Name", names.get(i)));
                clientEC2.createTags(createTagsRequest);
                i++;
            }
        }
    }
    
    public void createInstance(String name, String image)
    {
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
        runInstancesRequest.setInstanceType(InstanceType.T1Micro);
        runInstancesRequest.setImageId(image);
        runInstancesRequest.setKeyName(KEY_PAIR_NAME);
        runInstancesRequest.setMinCount(1);
        runInstancesRequest.setMaxCount(1);
        runInstancesRequest.setAdditionalInfo("programmatically set instance");
        
        // Add the security group to the request.
        ArrayList<String> securityGroupIds = new ArrayList<String>();
        securityGroupIds.add(SECURITY_GROUP_ID);
        runInstancesRequest.setSecurityGroupIds(securityGroupIds);
        
        
        // Launch the instance.
        RunInstancesResult runResult = clientEC2.runInstances(runInstancesRequest);
        
        List<Instance> ins = runResult.getReservation().getInstances();
        
        for (Instance instance : ins)
        {
            CreateTagsRequest createTagsRequest = new CreateTagsRequest();
            createTagsRequest.withResources(instance.getInstanceId()).withTags(new Tag("Name", name));
            clientEC2.createTags(createTagsRequest);
        }
        
    }
    
    
}