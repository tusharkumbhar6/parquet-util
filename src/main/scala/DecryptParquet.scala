import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object DecryptParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DecryptParquet").master("local").getOrCreate()
    decrypt(spark)
  }

  def decrypt(spark: SparkSession): Unit = {
    spark.sparkContext.hadoopConfiguration.set("parquet.encryption.kms.client.class"
      , "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS")

    // Explicit master keys (base64 encoded) - required only for mock InMemoryKMS
    spark.sparkContext.hadoopConfiguration.set("parquet.encryption.key.list"
      , "keyA:AAECAwQFBgcICQoLDA0ODw== ,  keyB:AAECAAECAAECAAECAAECAA==")

    // Activate Parquet encryption, driven by Hadoop properties
    spark.sparkContext.hadoopConfiguration.set("parquet.crypto.factory.class"
      , "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
    //spark.sparkContext.hadoopConfiguration.set("encryption key list", "k0:VHVzaGFyS3VtYmhhcg==,k1:QW5raXRhWWFkYXY=")
    //val ausDF = spark.read.parquet("src/main/resources/aus_population2.parquet.enc")
    val ausDF = spark.read
      .option("parquet.decryption.column.keys" , "keyA:state")
      //.option("parquet.encryption.column.keys" , "gccsa_sua:keyA")
      .option("parquet.decryption.footer.key" , "keyB")
      .parquet("src/main/resources/aus_population1.parquet.encrypted")
    ausDF.createOrReplaceTempView("aus_population")
    ausDF.persist(StorageLevel.MEMORY_ONLY)
    spark.sql("SELECT * FROM aus_population").show(100)
    //Thread.sleep(1000000)
  }

}
