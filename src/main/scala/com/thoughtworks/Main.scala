package com.thoughtworks

import java.util.UUID

import com.thoughtworks.dbutils.CassandraUtils


object Main {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Cluster address is required!")
      return
    }
    val clusterAddress = args(0)
    val session = CassandraUtils.getSession(clusterAddress, 9042)
    val keyspace = "de_training"

    //create db
    CassandraUtils.createKeyspace(keyspace, 3, session)

    //create tables
    CassandraUtils.createTable("users", Map("id" -> "UUID", "name" -> "text", "age" -> "int"),List("id"),
      List("name"), keyspace, session)

    //insert
    val key1 = UUID.randomUUID()
    val key2 = UUID.randomUUID()
    val values = List(
      Map("id" -> key1, "name" -> "Joe", "age" -> 25),
      Map("id" -> key2, "name" -> "Bob", "age" -> 34)
    )
    CassandraUtils.insert("users", values, keyspace, session)

    //update
    val keys = List("id", "name")
    val newValues = List(
      Map("id" -> key1, "name" -> "Joe", "age" -> 35),
      Map("id" -> key2, "name" -> "Bob", "age" -> 44)
    )
    CassandraUtils.update("users", keys, newValues, keyspace, session)

    //delete
    val deleteValues = List(
      Map("id" -> key2, "name" -> "Bob")
    )
    CassandraUtils.delete("users", keys, deleteValues, keyspace, session)

    //select
    val columns = List("id", "name", "age")
    val filter = Map("id" -> key1)
    val results = CassandraUtils.select("users", columns, filter, keyspace, session)
    results.foreach(x => println(x))

    val agreement = session.getCluster.getMetadata.checkSchemaAgreement()

    session.close()
    session.getCluster.close()

    println("Application exited.")
  }
}
