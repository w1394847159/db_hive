package com.imooc.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.{Connection, DriverManager, ResultSet, Statement}

object SparkConnHive {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local")
    /*conf.set("spark.sql.hive.metastore.jars","")
    conf.set("spark.sql.hive.metastore.version","1.2.2")*/


    val spark = SparkSession.builder()
      .appName("SparkConnHive")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val da = spark.read.textFile("hdfs://bigdata01:9000/hello.txt")
    spark.sql("use default")

    spark.sql("show tables")

    /*da.show()

    da.write.mode(SaveMode.Overwrite).saveAsTable("hello")
    println("写数据表成功")*/

    /*//jdbc的url
    val jdbcUrl = "jdbc:hive2://bigdata01:10000"
    //user为linux用户名，password可随意指定，不校验
    val conn = DriverManager.getConnection(jdbcUrl, "root", "root")

    val stmt = conn.createStatement

    //指定查询的sql
    var sql = "select * from student"

    sql = ""

    //执行sql
    val res = stmt.executeQuery(sql)

    //循环读取结果
    while ( {
      res.next
    }) System.out.println(res.getInt("id") + "\t" + res.getString("name"))
  }*/
  }
}
