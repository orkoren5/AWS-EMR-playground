
import java.io.File;
import java.util.ArrayList;
import java.util.List;

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

	private final static String ROOT_KEY_PATH = "./rootkey.csv";
	
	public static void main(String[] args) {

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
			List<StepConfig> steps = createStepsList();
			
			JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
			    .withInstanceCount(2)
			    .withMasterInstanceType(InstanceType.M1Small.toString())
			    .withSlaveInstanceType(InstanceType.M1Small.toString())
			    .withHadoopVersion("2.7.1").withEc2KeyName("home")
			    .withKeepJobFlowAliveWhenNoSteps(false)
			    .withPlacement(new PlacementType("us-east-1a"));
			 
			RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
			    .withName("jobname")
			    .withInstances(instances)
			    .withSteps(steps)
			    .withReleaseLabel("emr-4.7.0")
			    .withLogUri("s3n://ass2-or-yoed/log/")
			    .withServiceRole("EMR_DefaultRole")
			    .withJobFlowRole("EMR_EC2_DefaultRole");

			RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
			String jobFlowId = runJobFlowResult.getJobFlowId();
			System.out.println("Ran job flow with id: " + jobFlowId);

	}
	
	private static List<StepConfig> createStepsList() {
		
		List<StepConfig> steps = new ArrayList<StepConfig>();
		
		//////////////
		// Step 1
		/////////////
		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
	    .withJar("s3n://ass2-or-yoed/ass2.jar") // This should be a full map reduce application.
	    .withMainClass("Layer1")
	    .withArgs("s3n://ass2-or-yoed/input/", "s3n://ass2-or-yoed/step1_output/");
	 
		StepConfig stepConfig1 = new StepConfig()
		    .withName("layer1")
		    .withHadoopJarStep(hadoopJarStep1)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		steps.add(stepConfig1);
		
		//////////////
		// Step 2
		/////////////
		HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
	    .withJar("s3n://ass2-or-yoed/ass2.jar") // This should be a full map reduce application.
	    .withMainClass("Layer2")
	    .withArgs("s3n://ass2-or-yoed/step1_output/", "s3n://ass2-or-yoed/step2_output/");
	 
		StepConfig stepConfig2 = new StepConfig()
		    .withName("layer1")
		    .withHadoopJarStep(hadoopJarStep2)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		steps.add(stepConfig2);
		
		//////////////
		// Step 3
		/////////////
		HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
	    .withJar("s3n://ass2-or-yoed/ass2.jar") // This should be a full map reduce application.
	    .withMainClass("Layer3")
	    .withArgs("s3n://ass2-or-yoed/step2_output/", "s3n://ass2-or-yoed/step3_output/");
	 
		StepConfig stepConfig3 = new StepConfig()
		    .withName("layer1")
		    .withHadoopJarStep(hadoopJarStep3)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		steps.add(stepConfig3);
		
		//////////////
		// Step 4A
		/////////////
		HadoopJarStepConfig hadoopJarStep4A = new HadoopJarStepConfig()
	    .withJar("s3n://ass2-or-yoed/ass2.jar") // This should be a full map reduce application.
	    .withMainClass("Layer4A")
	    .withArgs("s3n://ass2-or-yoed/step3_output/", "s3n://ass2-or-yoed/step4A_output/");
	 
		StepConfig stepConfig4A = new StepConfig()
		    .withName("layer1")
		    .withHadoopJarStep(hadoopJarStep4A)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		steps.add(stepConfig4A);
		
		//////////////
		// Step 4B
		/////////////
		HadoopJarStepConfig hadoopJarStep4B = new HadoopJarStepConfig()
	    .withJar("s3n://ass2-or-yoed/ass2.jar") // This should be a full map reduce application.
	    .withMainClass("Layer4B")
	    .withArgs("s3n://ass2-or-yoed/step3_output/", "s3n://ass2-or-yoed/step4B_output/");
	 
		StepConfig stepConfig4B = new StepConfig()
		    .withName("layer1")
		    .withHadoopJarStep(hadoopJarStep4B)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		steps.add(stepConfig4B);
		
		return steps;
	}
}
