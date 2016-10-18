# Build Trapezium
Trapezium is a maven project. Following instructions will create Trapezium jar for your repository.
1. git clone <git_url>
2. mvn clean install

# Adding a maven dependency
Once Trapezium jar is available in your maven repository, you can add dependency on Trapezium using following dependency elements to your pom.xml

          <dependency>
            <groupId>com.verizon.bda</groupId>
            <artifactId>trapezium</artifactId>
            <version>1.0.0-SNAPSHOT</version>
          </dependency>

          <dependency>
            <groupId>com.verizon.bda</groupId>
            <artifactId> trapezium </artifactId>
            <type>test-jar</type>
            <scope>test</scope>
            <version>1.0.0-SNAPSHOT</version>
          </dependency>

# Setting up Cluster environment variable
On all your Spark nodes, create a file /opt/bda/environment and add environment for your cluster, e.g., DEV|QA|UAT|PROD. You can do this through a setup script so that any new node to your cluster will have this file automatically created. This file allows Trapezium to read data from different data sources or data locations based on your environment. 

Let's take an example. Suppose you read data from HDFS in your application. In your DEV cluster, data resides in /bda/**dev**/data while in your PROD cluster data resides in /bda/**prod**/data. Now environment file indicates Trapezium where to read data from.

# Creating test applications with Trapezium
### Example #1
Create an application that reads data from hdfs://my/first/trapezium/app/input and persists output to hdfs://my/first/trapezium/app/output. Data must be read in batch mode every 15 min. Any new data that has arrived since the last successful batch run must be presented as DataFrame to the application code. No validations are needed on the data source.

### Solution
1. Set up environment file correctly on your cluster (See **Setting up Cluster environment variable**)
2. Trapezium needs 2 config files

	**< ENVIRONMENT >_app_mgr.conf**

		appName = "MyTestApplication"
		persistSchema = "app_mgrTest_Schema"
		tempDir = "/tmp/"
		sparkConf = {
			kryoRegistratorClass = "com.verizon.bda.trapezium.framework.utils.EmptyRegistrator"
			spark.akka.frameSize = 100
		}

		zookeeperList = "localhost:2181"
		applicationStartupClass = "com.verizon.bda.trapezium.framework.apps.TestManagerStartup"
		fileSystemPrefix = "hdfs://"

	**< WORKFLOW_NAME >.conf**

		runMode = "BATCH"
		dataSource = "HDFS"
		hdfsFileBatch = {
			batchTime = 900
			timerStartDelay = 1
			batchInfo = [
    			{
		      		name = "source1"
				dataDirectory = {
					local = "test/data/local"
					dev = "test/data/dev"
					prod = "test/data/prod"
				}
			} ]
		}
		transactions = [{
			transactionName = "my.first.trapezium.application.TestTransaction"
			inputData = [{
    				name = "source1"
			}]
			persistDataName = "myFirstOutput"
		}]

3. Implement my.first.trapezium.application.Transaction scala object

		package my.first.trapezium.application
		object TestTransaction extends BatchTransaction {
			val logger = LoggerFactory.getLogger(this.getClass)
			override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {
				logger.info("Inside process of TestTransaction")
				require(df.size > 0)
				val inData = df.head._2
				**Your TRANSFORMATION code goes here**
				inData
			}
			override def persistBatch(df: DataFrame, batchTime: Time): Unit = {
				df.write.parquet("my/first/Trapezium/output")
			}
		}

4. Submitting your Trapezium job

	spark-submit --master yarn --class com.verizon.bda.trapezium.framework.ApplicationManager --config <configDir> --workflow <workflowName>

	ApplicationManager is your driver class. configDir is the location of your environment configuration file created in step #2. workflowName is your workflow config file that you created in step #2. workflow config file must be present under config directory.

	If you want to submit your job in cluster mode, environment and workflow config files must be present inside your application jar.

	config is an optional command line parameter while workflow is a required one.

(c) Verizon

Contributions from:

* Pankaj Rastogi (@rastogipankaj)
* Debasish Das (@debasish83)
* Hutashan Chandrakar
* Pramod Lakshmi Narasimha
* Sumanth Venkatasubbaiah
* Faraz Waseem
* Ken Tam
* Ponrama Jegan

And others (contact Pankaj Rastogi / Debasish Das if you've contributed code and aren't listed).
