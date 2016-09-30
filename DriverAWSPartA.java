import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class DriverAWSPartA 
{
	private static final int NUM_OF_INSTANCES = 20;
	private static final String TYPE_OF_INSTANCE = InstanceType.M3Xlarge.toString();
	static AmazonElasticMapReduce mapReduce;
	
	public static void main(String[] args) throws Exception 
	{
		String rnd = UUID.randomUUID().toString();
		
		try {
			AWSCredentials credentials = new PropertiesCredentials(new File("pro.properties"));
	        mapReduce = new AmazonElasticMapReduceClient(credentials);
	        mapReduce.setRegion(Region.getRegion(Regions.US_EAST_1));
	        System.out.println("## connect to AWS");
		} 
		catch (Exception e) { e.printStackTrace();	return;}
		
        List<StepConfig> stepsConfig = initLayers(rnd, args[0]);
        
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(NUM_OF_INSTANCES)
                .withMasterInstanceType(TYPE_OF_INSTANCE)
                .withSlaveInstanceType(TYPE_OF_INSTANCE)
                .withHadoopVersion("2.7.2")
                .withEc2KeyName("EC2_ass2")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        
        
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
				.withName("ass2")
				.withInstances(instances)
				.withSteps(stepsConfig)				
				.withLogUri("s3n://ass2/logs/")		   		 
        		.withJobFlowRole("EMR_EC2_DefaultRole")
        		.withServiceRole("EMR_DefaultRole");       	
        
        System.out.println("## job Flow: " + runFlowRequest.getName());
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		
		String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("## Ran job id: " + jobFlowId);

	}
	
	
	private static List<StepConfig> initLayers(String rnd, String kVal)
    {
        
		//arg1
		String outputPathL1= "s3n://ass2/output-Layer1" + rnd;
		String outputPathL2 = "s3n://ass2/output-Layer2" + rnd;
		String outputPathL3 = "s3n://ass2/output-Layer3" + rnd;
		String outputPathL4 = "s3n://ass2/output-Layer4" + rnd;
		
		
		//corpus
		//String arg0 = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/5gram/data"; 
        String arg0 =  "s3n://dsp112/eng.corp.10k";
        // TODO Change corpus
       
        
        HadoopJarStepConfig[] hadoopJarsStep = new HadoopJarStepConfig[] 
		{
			new HadoopJarStepConfig()
			.withJar("s3://ass2/Layer1.jar")
            .withMainClass("Layer1.class")
            .withArgs(arg0, outputPathL1),
            new HadoopJarStepConfig()
			.withJar("s3://ass2/Layer2.jar")
            .withMainClass("Layer2.class") 
            .withArgs(arg0, outputPathL2),
            new HadoopJarStepConfig()
			.withJar("s3://ass2/Layer3.jar")
            .withMainClass("Layer3.class")
            .withArgs(arg0, outputPathL3),
            new HadoopJarStepConfig()
			.withJar("s3://ass2/Layer4A.jar")
            .withMainClass("Layer4A.class") 
            .withArgs(arg0, outputPathL4, kVal)
		};
        
        
        List<StepConfig> stepsConfig = new LinkedList<>();
        stepsConfig.add(new StepConfig()
                .withName("Layer1")
                .withHadoopJarStep(hadoopJarsStep[0])
                .withActionOnFailure("TERMINATE_JOB_FLOW"));
        stepsConfig.add(new StepConfig()
                .withName("Layer2")
                .withHadoopJarStep(hadoopJarsStep[1])
                .withActionOnFailure("TERMINATE_JOB_FLOW"));
        stepsConfig.add(new StepConfig()
                .withName("Layer3")
                .withHadoopJarStep(hadoopJarsStep[2])
                .withActionOnFailure("TERMINATE_JOB_FLOW"));
        stepsConfig.add(new StepConfig()
                .withName("Layer4A")
                .withHadoopJarStep(hadoopJarsStep[3])
                .withActionOnFailure("TERMINATE_JOB_FLOW"));
        return stepsConfig;
    }
	

}
