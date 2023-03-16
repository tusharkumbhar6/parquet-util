
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object EncryptParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("EncryptParquet").master("local").getOrCreate()
    encrypt(spark)
  }

  def encrypt(spark: SparkSession): Unit = {
    //val ausDF: DataFrame = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("src/main/resources/aus_population.csv")

    //Rank,GCCSA_SUA,state,Population,Growth,Percentage_of_population
    val ausDF  = spark.read.schema(StructType(Seq(
      StructField("rank", IntegerType),
      StructField("gccsa_sua", org.apache.spark.sql.types.StringType),
      StructField("state", org.apache.spark.sql.types.StringType),
      StructField("population", org.apache.spark.sql.types.StringType),
      StructField("growth", org.apache.spark.sql.types.StringType),
      StructField("percentage", org.apache.spark.sql.types.StringType)
    ))).csv("src/main/resources/aus_population1.csv")

    spark.sparkContext.hadoopConfiguration.set("parquet.encryption.kms.client.class"
      , "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS")

    // Explicit master keys (base64 encoded) - required only for mock InMemoryKMS
    spark.sparkContext.hadoopConfiguration.set("parquet.encryption.key.list"
      , "keyA:AAECAwQFBgcICQoLDA0ODw== ,  keyB:AAECAAECAAECAAECAAECAA==")

    // Activate Parquet encryption, driven by Hadoop properties
    spark.sparkContext.hadoopConfiguration.set("parquet.crypto.factory.class"
      , "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")

    // Write encrypted dataframe files.
    // Column "square" will be protected with master key "keyA".
    // Parquet file footers will be protected with master key "keyB"


    ausDF.na.fill("null_replaced").write
      //.option("parquet.encryption.column.keys" , "keyA:state")
      //.option("parquet.encryption.footer.key" , "keyB")
      .mode("overwrite")
      .parquet("src/main/resources/aus_population1.parquet.encrypted")


    //Thread.sleep(1000000)
    //print("ok")
  }
}
