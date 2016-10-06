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
	private static final int NUM_OF_INSTANCES = 2;
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
                .withEc2KeyName("emr-yoed")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        
        
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
				.withName("ass2")
				.withReleaseLabel("emr-4.7.0")
				.withInstances(instances)
				.withSteps(stepsConfig)				
				.withLogUri("s3n://yoed-or-two/logs/")		   		 
        		.withJobFlowRole("EMR_EC2_DefaultRole")
        		.withServiceRole("EMR_DefaultRole");       	
        
        System.out.println("## job Flow: " + runFlowRequest.getName());
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		
		String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("## Run job id: " + jobFlowId);

	}
	
	
	private static List<StepConfig> initLayers(String rnd, String kVal)
    {
        
		//arg1
		String outputPathL1= "s3n://yoed-or-two/output/output-Layer1" + rnd;
		String outputPathL2 = "s3n://yoed-or-two/output/output-Layer2" + rnd;
		String outputPathL3 = "s3n://yoed-or-two/output/output-Layer3" + rnd;
		String outputPathL4 = "s3n://yoed-or-two/output/output-Layer4" + rnd;
		
		
		//corpus
		//String arg0 = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/5gram/data"; 
        //String arg0 =  "s3://dsp112/eng.corp.10k";
		String arg0 =  "s3n://yoed-or-two/eng.corp.10k";
        // TODO Change corpus
       
        
        HadoopJarStepConfig[] hadoopJarsStep = new HadoopJarStepConfig[] 
		{
			new HadoopJarStepConfig()
			.withJar("s3n://yoed-or-two/Layer1.jar")
            .withMainClass("Layer1.class")
            .withArgs(arg0 , outputPathL1),
            new HadoopJarStepConfig()
			.withJar("s3n://yoed-or-two/Layer2.jar")
            .withMainClass("Layer2.class") 
            .withArgs(outputPathL1, outputPathL2),
            new HadoopJarStepConfig()
			.withJar("s3n://yoed-or-two/Layer3.jar")
            .withMainClass("Layer3.class")
            .withArgs(outputPathL2, outputPathL3),
            new HadoopJarStepConfig()
			.withJar("s3n://yoed-or-two/Layer4A.jar")
            .withMainClass("Layer4A.class") 
            .withArgs(outputPathL3, outputPathL4, kVal)
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
