

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
//Spark connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

object ReadTable extends App {

 /* cqlsh> CREATE KEYSPACE prasanna WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
  cqlsh> use prasanna;
  cqlsh:prasanna> CREATE TABLE emp(emp_id int PRIMARY KEY, emp_name text ,emp_city text,emp_sal varint, emp_phone varint);
  cqlsh:prasanna> INSERT INTO emp (emp_id, emp_name, emp_city, emp_phone, emp_sal) VALUES(1,'ram', 'Hyderabad', 9035749918, 50000);
  cqlsh:prasanna> select *from prasanna.emp;

  emp_id | emp_city  | emp_name | emp_phone  | emp_sal
  --------+-----------+----------+------------+---------
  1 | Hyderabad |      ram | 9035749918 |   50000

  (1 rows)
  */

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

  df.show();

}
