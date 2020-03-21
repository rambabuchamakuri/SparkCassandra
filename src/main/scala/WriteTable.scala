
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._
//Spark connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

object WriteTable extends App{

  val spark = SparkSession
    .builder()
    .appName("SparkCassandraApp")
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", "9042")
    .master("local[2]")
    .getOrCreate();

  val df = spark.read.format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "emp", "keyspace" -> "prasanna"))
    .load()


  //val updateDf = df.withColumn("emp_id", when(col("student_type").equalTo("1"), lit("Day-Scholar")).otherwise("Hostler"));

  // update record method 1
  df.createOrReplaceTempView("temp")
  val tempview =spark.sql("select emp_id+1 as emp_id, \"prasanna\" as emp_name, emp_city,emp_phone, emp_sal from temp where emp_id=1")
  tempview.show()

  //update record method 2
  val updateDf = df.select( df("emp_id")+1 as("emp_id")  , df("emp_city"),  df("emp_name"),df("emp_phone"),  df("emp_sal"))
      .filter(df("emp_id") === 1)

    updateDf.show()

  updateDf.write.cassandraFormat("emp", "prasanna").mode(SaveMode.Append).save();

  //Read Cassandra data using DataFrame
 //val df_highSal = spark.read.cassandraFormat("emp", "prasanna").load()


  //Display all high salary employees
  println("All high salary employees: ")
// df_highSal.show()
}
