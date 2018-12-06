package com.thoughtworks.dbutils

import com.datastax.driver.core.{Cluster, DataType, Row, Session}

import scala.collection.mutable

object CassandraUtils {
  def createKeyspace(name: String, replicationFactor: Integer, session: Session) = {
    session.execute(
      s"""CREATE KEYSPACE IF NOT EXISTS $name
         WITH REPLICATION = {
         'class' : 'SimpleStrategy',
         'replication_factor' : $replicationFactor
         };""")
  }

  def getSession(nodes: String): Session = {
    val cluster = Cluster.builder()
      .addContactPoint(nodes)
      .build()
    cluster.connect()
  }

  def createTable(name: String, columns: Map[String, String], partitionKeys: List[String],
                  clusteringColumns: List[String], keyspace: String, session: Session) = {
    val columnsStr = columns.map(x => s"${x._1} ${x._2}").mkString(", ")
    val partitionKeysStr = partitionKeys.mkString("(", ", ", ")")
    val clusteringColumnsStr = clusteringColumns.mkString(", ")
    session.execute(
      s"""
        CREATE TABLE IF NOT EXISTS $keyspace.$name (
        $columnsStr,
        PRIMARY KEY ($partitionKeysStr, $clusteringColumnsStr)
        )
      """)
  }

  private def addQuotes(x: Iterable[Any]): Iterable[Any] = {
    x.map(x => addQuotes(x))
  }

  private def addQuotes(x: Any): Any = {
    if (x.isInstanceOf[String]) s"'$x'" else x
  }

  def insert(tableName: String, data: Map[String, Any], keyspace: String, session: Session) = {
    val columnNamesStr = data.keys.mkString("(", ",", ")")
    val valuesStr = addQuotes(data.values).mkString("(", ",", ")")
    session.execute(s"""INSERT INTO $keyspace.$tableName $columnNamesStr values $valuesStr""")
  }

  def insert(tableName: String, data: List[Map[String, Any]], keyspace: String, session: Session): Unit = {
    data.foreach(x => insert(tableName, x, keyspace, session))
  }

  def update(tableName: String, keys: List[String], data: Map[String, Any], keyspace: String, session: Session): Unit = {
    val keysStr = keys.map(x => s"$x = ${addQuotes(data(x))}").mkString(" AND ")
    val valuesStr = data.filter(x => !keys.contains(x._1)).map(x => s"${x._1} = ${addQuotes(x._2)}").mkString(", ")
    session.execute(s"""UPDATE $keyspace.$tableName SET $valuesStr WHERE $keysStr""")
  }

  def update(tableName: String, keys: List[String], data: List[Map[String, Any]], keyspace: String,
             session: Session): Unit = {
    data.foreach(x => update(tableName, keys, x, keyspace, session))
  }

  def delete(tableName: String, keys: List[String], data: Map[String, Any], keyspace: String, session: Session) = {
    val keysStr = keys.map(x => s"$x = ${addQuotes(data(x))}").mkString(" AND ")
    session.execute(s"""DELETE FROM $keyspace.$tableName WHERE $keysStr""")
  }

  def delete(tableName: String, keys: List[String], data: List[Map[String, Any]], keyspace: String, session: Session): Unit = {
    data.foreach(x => delete(tableName, keys, x, keyspace, session))
  }

  def select(tableName: String, columns: List[String], filter: Map[String, Any], keyspace: String, session: Session) = {
    val columnsStr = columns.mkString(",")
    val filterStr = filter.map(x => s"${x._1} = ${addQuotes(x._2)}").mkString(" AND ")
    val result = session.execute(s"SELECT $columnsStr FROM $keyspace.$tableName WHERE $filterStr")
    val results: mutable.MutableList[Map[String, Any]] = mutable.MutableList()
    result.forEach(row => results += columns.map(x => x -> getValue(row, x)).toMap)
    results
  }

  private def getValue(row: Row, column: String): Any = {
    val typez = row.getColumnDefinitions.getType(column)
    if (typez == DataType.text())
      row.getString(column)
    else if (typez == DataType.cint())
      row.getInt(column)
    else if (typez == DataType.uuid())
      row.getUUID(column)
  }
}
