import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.param._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.PipelineModel



/*object IAGDemoApp { 
	def main(args: Array[string]) {*/

		// create a spark seesion entry point
		val sparkSession = SparkSession.builder.
							master("local").
							appName("IAG Demo").
							enableHiveSupport().
							getOrCreate()

		
		import sparkSession.implicits._

		//the working directory
		val working_dir = "/home/nejhdeh/Spark/Scala_tutorials/IAG_Demo/"
		val model_name = "logit_model3"

		//read the data as dataframe - this is better method since takes care of headers
		val IAGDataAllDF = sparkSession.read.option("header","true").csv(working_dir + "CreditScoringDataLinear_all.txt")

		//reduce the dataframe
		val IAGDataRawDF = IAGDataAllDF.select("age", "account_age", "ict_ve_cntr", "years_mil_service", "account_fees_prev_year", "applied_online", "account_type", "city_code", "loyalty_class", "car_loan_holder", "higher_education", "home_loan_holder", "eligibility_class", "y")

		
		val IAGDataDF = IAGDataRawDF.withColumn("age", IAGDataRawDF("age").cast(DoubleType)).
		withColumn("account_age", IAGDataRawDF("account_age").cast(DoubleType)).
		withColumn("ict_ve_cntr", IAGDataRawDF("ict_ve_cntr").cast(DoubleType)).
		withColumn("years_mil_service", IAGDataRawDF("years_mil_service").cast(DoubleType)).
		withColumn("account_fees_prev_year", IAGDataRawDF("account_fees_prev_year").cast(IntegerType)).
		withColumn("applied_online", IAGDataRawDF("applied_online").cast(StringType)).
		withColumn("account_type", IAGDataRawDF("account_type").cast(IntegerType)).
		withColumn("city_code", IAGDataRawDF("city_code").cast(IntegerType)).
		withColumn("loyalty_class", IAGDataRawDF("loyalty_class").cast(IntegerType)).
		withColumn("car_loan_holder", IAGDataRawDF("car_loan_holder").cast(StringType)).
		withColumn("higher_education", IAGDataRawDF("higher_education").cast(StringType)).
		withColumn("home_loan_holder", IAGDataRawDF("home_loan_holder").cast(StringType)).
		withColumn("eligibility_class", IAGDataRawDF("eligibility_class").cast(IntegerType)).
		withColumn("y", IAGDataRawDF("y").cast(IntegerType))
		


		//split the data
		val Array(trainIAGDataDF, testIAGDataDF) = IAGDataDF.randomSplit(Array(0.7,0.3))

		// ML Transformation
		////////////////////
		//set the vals for the features
		val numericFeatureColNames = Array("age", "account_age", "ict_ve_cntr", "years_mil_service", "account_fees_prev_year")
		val categoricalFeatureColNames = Array("applied_online", "account_type", "city_code", "loyalty_class", "car_loan_holder", "higher_education", "home_loan_holder", "eligibility_class")
		
		//set the label given in table
		val labelColName = "y"
		val featuresColName = "features"


		// cobmine the column names
		val allFeaturesColNames = numericFeatureColNames ++ categoricalFeatureColNames
		val allIdFeatureColNames = numericFeatureColNames ++ idCategoricalColNames


		//run through he string indexer to convert categorical to double as required by Spark ML
		//val stringIndexers = categoricalFeatureColNames.map(colName => new StringIndexer().setInputCol(colName).setOutputCol(s"${colName}_indexed").fit(IAGDataDF))
		val stringIndexers = categoricalFeatureColNames.map(colName => new StringIndexer().setInputCol(colName).setOutputCol(s"${colName}_indexed"))

		//now for the label
		//val labelIndexer = new StringIndexer().setInputCol(labelColName).setOutputCol(labelColName + "_indexed").fit(IAGDataDF)
		//note: calling it "y_indexed" and not "label"
		val labelIndexer = new StringIndexer().setInputCol(labelColName).setOutputCol(labelColName + "_indexed")
		
		//vector assembler - name the output "features" since will be used by logit
		val assembler = new VectorAssembler().setInputCols(Array(allIdFeatureColNames: _*)).setOutputCol(featuresColName)


		//ML Pipeline
		//define the pipeline  -order of operations for ML
		//////////////////////////////////////////////////
		val pipeline_DFOnly = new Pipeline().setStages(Array.concat(stringIndexers.toArray) ++ Array(labelIndexer, assembler))

		//save the revised dataframe
		val IAGDataDF_Featurised = pipeline_DFOnly.fit(IAGDataDF).transform(IAGDataDF)
	

		//split the featurised data into training and test sets
		val Array(train_IAGDataDF_Featurised, test_IAGDataDF_Featurised) = IAGDataDF_Featurised.randomSplit(Array(0.7,0.3))

		
		//logistic regression estimator
		val logisticRegression = new LogisticRegression().
			setMaxIter(20).
			setRegParam(0.01).
			setFeaturesCol(featuresColName).
			setLabelCol(labelColName + "_indexed")


		// Develop the logit model
		val model = logisticRegression.fit(train_IAGDataDF_Featurised)

		//print the model coefficients 
		println("model coefficients and intercept are: " + model.coefficients + " " + model.intercept)

		//save the model
		model.save(working_dir + model_name)


		//MODEL EVALUATION
		//////////////////
		//load the saved model back in
		val model = LogisticRegressionModel.load(working_dir + model_name)

		//apply the model on the original training set to get performance
		val trainingPredictions = model.transform(train_IAGDataDF_Featurised)

		//apply the model on the test data sets
		val testPredictions = model.transform(test_IAGDataDF_Featurised)

		//Evaluate the model using the AUC metrics
		//note: must also tell it the label name since its "y_indexed" and not "label"
		val evaluator = new BinaryClassificationEvaluator().setLabelCol(labelColName + "_indexed")


		//setup the parameter map
		val evaluatorParamMap = ParamMap(evaluator.metricName -> "areaUnderROC")
		
		//traing set
		val trainingAUC = evaluator.evaluate(trainingPredictions, evaluatorParamMap)

		//test set
		val testAUC = evaluator.evaluate(testPredictions, evaluatorParamMap)

		//print the AUC results
		println("training AUC " + trainingAUC)
		println("test AUC " + testAUC)

		
		//CROSS VALIDATION
		//////////////////

		//re-run the Logistic 
		val logisticRegressionEval = new LogisticRegression().
			setFeaturesCol(featuresColName).
			setLabelCol(labelColName + "_indexed")


		//create a new pipeline but include the logit also since part of evaluation process
		//val pipeline_withLogit = new Pipeline().setStages(Array.concat(stringIndexers.toArray) ++ Array(labelIndexer, assembler) ++ Array(logisticRegressionEval))
		
		val pipeline_withLogit = new Pipeline().setStages(Array(logisticRegressionEval))
		//set up the grid of values to perform cross validation
		val paramGrid = new ParamGridBuilder().
			addGrid(logisticRegressionEval.regParam, Array(0.01,0.1,1.0)).
			addGrid(logisticRegressionEval.maxIter, Array(20,30)).
			build()

		//create a new evaluator since since its for best model
		val evaluator_CrossModel = new BinaryClassificationEvaluator().setLabelCol(labelColName + "_indexed")

		//The cross validator
		val crossValidator = new CrossValidator().
			setEstimator(pipeline_withLogit).
			setEstimatorParamMaps(paramGrid).
			setNumFolds(5).
			setEvaluator(evaluator_CrossModel)

		//cross validator model(s) with training
		val crossValidatorModel = crossValidator.fit(train_IAGDataDF_Featurised)

		//Model to predict multiple predictions on test data
		val newPredictions = crossValidatorModel.transform(test_IAGDataDF_Featurised)

		//The new evaluator metrics - similar to the single model
		val evaluatorCorssValParamMap = ParamMap(evaluator_CrossModel.metricName -> "areaUnderROC")

		//calc the AUC
		val newTestAUC = evaluator_CrossModel.evaluate(newPredictions, evaluatorCorssValParamMap)
		println("new AUC (with Cross Validation) " + newTestAUC)

		//Obtain the best model
		val bestModel = crossValidatorModel.bestModel

		//Summary of the best model
		///////////////////////////
		println()
		println("Parameters for the Best Model")

		val bestPipelineModel = bestModel.asInstanceOf(PipelineModel)
		val stages = bestPipelineModel.stages

		//show number of stages
		println("Best Model evaluation number of stages: " + stages.length)

		//get the logistic regression stage
		val logitStage = stages(14).asInstanceOf(LogisticRegressionModel)

		println("regParam = " + logitStage.getRegParam)

		//wrting results to disk
		//IAGDataDF.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/nejhdeh/Spark/Scala_tutorials/IAG_Demo/test")


	//	}


	//}



