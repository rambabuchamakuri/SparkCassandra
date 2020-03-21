
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._
//Spark connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

object WriteTableFromRDD extends App{

  //Prerequiste :  table should be created with the below metadata

  val spark = SparkSession
    .builder()
    .appName("SparkCassandraApp")
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", "9042")
    .master("local[2]")
    .getOrCreate();

  val empData = Seq((4,"Yeswanth","Kattubadivaripalem",9705813982L,5000),(5,"Gopi","Guntur",9493060245L,5000))
  val empRdd   =  spark.sparkContext.parallelize(empData)
  val empDf =   spark.createDataFrame(empRdd).toDF("emp_id","emp_name","emp_city","emp_phone","emp_sal")
  empDf.show()

  empDf.write.cassandraFormat("emp", "prasanna").mode(SaveMode.Append).save();
}
