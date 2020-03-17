package com.datastax.examples


import java.sql.DriverManager


object ClusterConnect {

  def main(args: Array[String]):Unit = {
    new ClusterQuery()
  }

  val url = "jdbc:spark://AOSSStatusEndpoints=127.0.0.1:9077;"

  Class.forName("com.simba.spark.jdbc41.Driver")

  val con = DriverManager.getConnection(url,"", "")
  val stmt = con.createStatement()

  class ClusterQuery() {

    val query = "SELECT * FROM keyspace.table WHERE key = 10"

    val top = System.currentTimeMillis
    val test = stmt.executeQuery(query)

    val count = test.getMetaData().getColumnCount()

    while (test.next()){
      println(test.getString(1))
    }

    val current = (System.currentTimeMillis() - top).toString
    println(s"Time: $current")

  }

}
