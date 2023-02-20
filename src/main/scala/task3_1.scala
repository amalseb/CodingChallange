import commons.{Common, Constants}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

object Task3_1 {

  def main(args: Array[String]): Unit = {
    val spark = Common.getSparkSession(Constants.TASK3_1)

    // Read Csv file and create dataframe
    val irisDf = spark.read.format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(Constants.PART3_DIR)
      .toDF(Constants.SEPAL_LENGTH, Constants.SEPAL_WIDTH, Constants.PETAL_LENGTH, Constants.PETAL_WIDTH, Constants.CLASS)

    //encode the class column to numeric values
    val indexer = new StringIndexer()
      .setInputCol(Constants.CLASS)
      .setOutputCol(Constants.LABEL)
    val indexed = indexer.fit(irisDf).transform(irisDf)

    // Create feature vector using Vector Assembler
    val assembler = new VectorAssembler()
      .setInputCols(Array(Constants.SEPAL_LENGTH, Constants.SEPAL_WIDTH, Constants.PETAL_LENGTH, Constants.PETAL_WIDTH))
      .setOutputCol(Constants.FEATURES)

    val dfWithFeatures = assembler.transform(indexed)

    // Split data into training and test sets in ration of 70% and 30% respectively
    val Array(trainingIrisData, testIrisData) = dfWithFeatures.randomSplit(Array(0.7,0.3))

    //Fit logistic regression model
    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.0)
      .setElasticNetParam(0.0)

    val lrModel = lr.fit(trainingIrisData)

    //Make predictions on test data
    val predictions = lrModel.transform(testIrisData)

    // Create a mapping DataFrame for the class names
    import spark.implicits._
    val classMap = Seq(
      (Constants.IRIS_SETOSA, 0.0),
      (Constants.IRIS_VIRGINICA, 1.0),
      (Constants.IRIS_VERSICOLOR, 2.0)
    ).toDF(Constants.CLASS_NAME, Constants.CLASS_LABEL)

    val predictedWithClassNames = predictions
      .join(classMap, $"prediction" === $"class_label", "left")
      .select(Constants.SEPAL_LENGTH, Constants.SEPAL_WIDTH, Constants.PETAL_LENGTH, Constants.PETAL_WIDTH, Constants.PREDICTION, Constants.CLASS_NAME)

    // Show the DataFrame with the predicted class names
    predictedWithClassNames.show()
  }
}
