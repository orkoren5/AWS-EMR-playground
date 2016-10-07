
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
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

public class EMRDriver {

	private final static String ROOT_KEY_PATH = "pro.properties";
	
	public static void main(String[] args) {
			
			String rnd = UUID.randomUUID().toString();

	 	    AWSCredentials credentials = null; 	   
		    try {	    	
		    	System.out.println("Loading credentials...");
		    	credentials = new PropertiesCredentials(new File(ROOT_KEY_PATH));	    	
		        //credentials = new ProfileCredentialsProvider("default").getCredentials();
		        System.out.println("Credentials Loaded. Key: " + credentials.getAWSAccessKeyId());
		    } catch (Exception e) {
		        throw new AmazonClientException(
		                "Cannot load the credentials from the credential profiles file. " +
		                "Please make sure that your credentials file is at the correct " +
		                "location (C:\\Users\\Or\\.aws\\credentials), and is in valid format.",
		                e);
		    }
		    
			AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
			mapReduce.setRegion(Region.getRegion(Regions.US_EAST_1));
			List<StepConfig> steps = createStepsList(rnd, args[0],args[1]);
			
			JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
			    .withInstanceCount(2)
			    .withMasterInstanceType(InstanceType.M1Small.toString())
			    .withSlaveInstanceType(InstanceType.M1Small.toString())
			    .withHadoopVersion("2.7.2").withEc2KeyName("emr-yoed")
			    .withKeepJobFlowAliveWhenNoSteps(false)
			    .withPlacement(new PlacementType("us-east-1a"));
			 
			RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
			    .withName("jobname")
			    .withInstances(instances)
			    .withSteps(steps)
			    .withReleaseLabel("emr-4.7.0")
			    .withLogUri("s3n://yoed-or-two/logs/")
			    .withServiceRole("EMR_DefaultRole")
			    .withJobFlowRole("EMR_EC2_DefaultRole");

			RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
			String jobFlowId = runJobFlowResult.getJobFlowId();
			System.out.println("Ran job flow with id: " + jobFlowId);

	}
	
	private static List<StepConfig> createStepsList(String rnd, String kVal , String th) {
		
		List<StepConfig> steps = new ArrayList<StepConfig>();
		
		//arg1
				String outputPathL1= "s3n://yoed-or-two/output/output-Layer1" + rnd;
				String outputPathL2 = "s3n://yoed-or-two/output/output-Layer2" + rnd;
				String outputPathL3 = "s3n://yoed-or-two/output/output-Layer3" + rnd;
				String outputPathL4A = "s3n://yoed-or-two/output/output-Layer4A" + rnd;
				String outputPathL4B = "s3n://yoed-or-two/output/output-Layer4B" + rnd;
				
		
				//corpus
				String arg0 = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/5gram/data"; 
		        //String arg0 =  "s3://dsp112/eng.corp.10k";
				//String arg0 =  "s3n://yoed-or-two/eng.corp.10k";
		        // TODO Change corpus
				
				
		//////////////
		// Step 1
		/////////////
		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
				.withJar("s3n://yoed-or-two/Layer1.jar")
	            .withMainClass("Layer1.class")
	            .withArgs(arg0 , outputPathL1);
        
	 
		StepConfig stepConfig1 = new StepConfig()
		    .withName("layer1")
		    .withHadoopJarStep(hadoopJarStep1)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		steps.add(stepConfig1);
				
		//////////////
		// Step 2
		/////////////
		HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
				.withJar("s3n://yoed-or-two/Layer2.jar")
	            .withMainClass("Layer2.class") 
	            .withArgs(outputPathL1, outputPathL2);
	 
		StepConfig stepConfig2 = new StepConfig()
		    .withName("layer2")
		    .withHadoopJarStep(hadoopJarStep2)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		steps.add(stepConfig2);
		
		//////////////
		// Step 3
		/////////////
		HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
				.withJar("s3n://yoed-or-two/Layer3.jar")
	            .withMainClass("Layer3.class")
	            .withArgs(outputPathL2, outputPathL3);
	 
		StepConfig stepConfig3 = new StepConfig()
		    .withName("layer3")
		    .withHadoopJarStep(hadoopJarStep3)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		steps.add(stepConfig3);
		
		//////////////
		// Step 4A
		/////////////
		HadoopJarStepConfig hadoopJarStep4A = new HadoopJarStepConfig()
				.withJar("s3n://yoed-or-two/Layer4A.jar")
	            .withMainClass("Layer4A.class") 
	            .withArgs(outputPathL3, outputPathL4A, kVal);
	 
		StepConfig stepConfig4A = new StepConfig()
		    .withName("layer4A")
		    .withHadoopJarStep(hadoopJarStep4A)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		steps.add(stepConfig4A);
		
		//////////////
		// Step 4B
		/////////////
		HadoopJarStepConfig hadoopJarStep4B = new HadoopJarStepConfig()
				.withJar("s3n://yoed-or-two/Layer4B.jar")
	            .withMainClass("Layer4B.class") 
	            .withArgs(outputPathL3, outputPathL4B, th);
	 
		StepConfig stepConfig4B = new StepConfig()
		    .withName("layer4B")
		    .withHadoopJarStep(hadoopJarStep4B)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		steps.add(stepConfig4B);
		
		return steps;
	}
}
