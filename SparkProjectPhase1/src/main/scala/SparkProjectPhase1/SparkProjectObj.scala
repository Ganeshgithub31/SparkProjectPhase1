package SparkProjectPhase1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkProjectObj {

	def main(args:Array[String]):Unit={
			val conf=new SparkConf().setAppName("First").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("Error")

					val spark=SparkSession.builder().getOrCreate()
					import spark.implicits._

					val Avrodf=spark.read.format("com.databricks.spark.avro")
					.load("file:///E:/Hadoop/Spark/Spark Project Phase 1/part-00000-1bd5ec9a-4ceb-448c-865f-305f43a0b8a9-c000.avro")

					println("====== Avro Data Read=======")

					Avrodf.show(10,false)

					val url="https://randomuser.me/api/0.8/?results=200"
					val urldata=scala.io.Source.fromURL(url).mkString
					val RDDdata=sc.parallelize(List(urldata))
					val urlapidf=spark.read.json(RDDdata)

					println
					println("========= API Raw data====")

					urlapidf.show(10,false)
					urlapidf.printSchema()		

					val flatendata=urlapidf.select(col("nationality"),col("results"),col("seed"),col("version") )

					println
					println("========= Flatten data====")

					flatendata.show(5,false)
					flatendata.printSchema()

					val explodedf=flatendata.withColumn("results", explode(col("results")))

					println
					println("========= explodedf data====")

					explodedf.show(10,false)
					explodedf.printSchema()


					val finalexplodedf=explodedf.select(
							col("nationality"),    
							col("results.user.cell"),
							col("results.user.dob"),
							col("results.user.email"),
							col("results.user.gender"),
							col("results.user.location.city"),
							col("results.user.location.state"),
							col("results.user.location.street"),
							col("results.user.location.zip"),
							col("results.user.md5"),
							col("results.user.name.first"),
							col("results.user.name.last"),
							col("results.user.name.title"),
							col("results.user.password"),
							col("results.user.phone"),
							col("results.user.picture.large"),
							col("results.user.picture.medium"),
							col("results.user.picture.thumbnail"),
							col("results.user.registered"),
							col("results.user.salt"),
							col("results.user.sha1"),
							col("results.user.sha256"),
							col("results.user.username"),
							col("seed"),    
							col("version")			
							)

					println
					println("========= final explodedf data====")

					finalexplodedf.show(10,false)
					finalexplodedf.printSchema()

					println
					println("========= Removing of Numerical from username====")


					val Finaldf=finalexplodedf.withColumn("username",regexp_replace(col("username"), "([0-9])",""))
					Finaldf.show(10,false)


					val joindataframe=Avrodf.join(broadcast(Finaldf),Seq("username"),"left")

					println
					println("========= broadcast join for Avro and API data using username====")

					joindataframe.show(10,false)


					val not_null_nationality_df=joindataframe.filter("Nationality is not null")
					println
					println("===========NOT Null Nationality records==========")
					not_null_nationality_df.show(5,false)

					val null_nationality_df=joindataframe.filter("Nationality is null")
					println
					println("===========null Nationality records==========")
					null_nationality_df.show(5,false)		

					println
					println("===========null handling records==========")


					val handlenulldf=null_nationality_df.na.fill(0).na.fill("NA")
					handlenulldf.show(5,false)

					val add_date_null_col_df=handlenulldf.withColumn("today_date", current_date())
					val add_date_notnull_col_df=not_null_nationality_df.withColumn("today_date", current_date())

					println
					println("===========Adding date col to Null df at the end==========")

					add_date_null_col_df.show(5)

					println
					println("===========Adding date col to Not Null df at the end==========")

					add_date_notnull_col_df.show(5)

					val final_null_df=add_date_null_col_df.groupBy("username").agg(
							collect_list("ip").alias("ip"),
							collect_list("id").alias("id"),
							sum("amount").cast(new DecimalType(38,2)).alias("total_sum"),
							struct(
									count("ip").alias("ip_count"),
									count("id").alias("id_count")
									).alias("count"))


					println()
					println("=========Null data written to Local and Aggregate operation=========")

					final_null_df.show()

					final_null_df.coalesce(1).write.format("json").mode("overwrite")
					.save("file:///E:/Hadoop/Spark/Spark Project Phase 1/final_null_df")

					val final_notnull_df=add_date_notnull_col_df.groupBy("username").agg(
							collect_list("ip").alias("ip"),
							collect_list("id").alias("id"),
							//sum("amount").cast(new DecimalType(38,2)).alias("total_sum"),
							sum("amount").cast(DataTypes.createDecimalType(18,2)).alias("total_sum"),

							struct(
									count("ip").alias("ip_count"),
									count("id").alias("id_count")
									).alias("count"))


					println()
					println("=========Not Null data written to Local and Aggregate operation========")

					final_notnull_df.coalesce(1).write.format("json").mode("overwrite")
					.save("file:///E:/Hadoop/Spark/Spark Project Phase 1/final_not_null_df")

					final_notnull_df.show()

	}

}