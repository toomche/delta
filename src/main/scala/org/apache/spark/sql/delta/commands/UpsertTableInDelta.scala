package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.delta.actions.{Action, AddFile}
import org.apache.spark.sql.delta.schema.ImplicitMetadataOperation
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaOptions, OptimisticTransaction}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, ShortType}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession, functions => F}

case class UpsertTableInDelta(data: Dataset[_],
                              deltaLog: DeltaLog,
                              options: DeltaOptions,
                              partitionColumns: Seq[String],
                              configuration: Map[String, String]
                             ) extends RunnableCommand
  with ImplicitMetadataOperation
  with DeltaCommand with DeltaCommandsFun {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(configuration.contains(UpsertTableInDelta.ID_COLS), "idCols is required ")
    var actions = Seq[Action]()
    deltaLog.withNewTransaction { txn =>
      actions = upsert(txn, sparkSession)
      val operation = DeltaOperations.Write(SaveMode.Overwrite,
        Option(partitionColumns),
        options.replaceWhere)
      txn.commit(actions, operation)
    }

    if (actions.size == 0) Seq[Row]() else {
      actions.map(f => Row.fromSeq(Seq(f.json)))
    }
  }

  def upsert(txn: OptimisticTransaction, sparkSession: SparkSession): Seq[Action] = {

    import sparkSession.implicits._
    val snapshot = deltaLog.snapshot
    val metadata = deltaLog.snapshot.metadata

    /**
      * Firstly, we should get all partition columns from `idCols` condition.
      * Then we can use them to optimize file scan.
      */
    val idCols = configuration.getOrElse(UpsertTableInDelta.ID_COLS, "")
    val idColsList = idCols.split(",").filterNot(_.isEmpty).toSeq
    val partitionColumnsInIdCols = partitionColumns.intersect(idColsList)


    val partitionFilters = if (partitionColumnsInIdCols.size > 0) {
      val schema = data.schema

      def isNumber(column: String) = {
        schema.filter(f => f.name == column).head.dataType match {
          case _: LongType => true
          case _: IntegerType => true
          case _: ShortType => true
          case _: DoubleType => true
          case _ => false
        }
      }

      val minMaxColumns = partitionColumnsInIdCols.flatMap { column =>
        Seq(F.lit(column), F.min(column).as(s"${column}_min"), F.max(F.max(s"${column}_max")))
      }.toArray
      val minxMaxKeyValues = data.select(minMaxColumns: _*).collect()

      // build our where statement
      val whereStatement = minxMaxKeyValues.map { row =>
        val column = row.getString(0)
        val minValue = row.get(1).toString
        val maxValue = row.get(2).toString

        if (isNumber(column)) {
          s"${column} >= ${minValue} and   ${maxValue} >= ${column}"
        } else {
          s"""${column} >= "${minValue}" and   "${maxValue}" >= ${column}"""
        }
      }
      logInfo(s"whereStatement: ${whereStatement.mkString(" and ")}")
      val predicates = parsePartitionPredicates(sparkSession, whereStatement.mkString(" and "))
      Some(predicates)

    } else None


    val filterFilesDataSet = partitionFilters match {
      case None =>
        snapshot.allFiles
      case Some(predicates) =>
        DeltaLog.filterFileList(
          metadata.partitionColumns, snapshot.allFiles.toDF(), predicates).as[AddFile]
    }

    // Again, we collect all files to driver,
    // this may impact performance and even make the driver OOM when
    // the number of files are very huge.
    // So please make sure you have configured the partition columns or make compaction frequently

    val filterFiles = filterFilesDataSet.collect
    val dataInTableWeShouldProcess = deltaLog.createDataFrame(snapshot, filterFiles, false, None)

    val dataInTableWeShouldProcessWithFileName = dataInTableWeShouldProcess.select(
      F.input_file_name().as(UpsertTableInDelta.FILE_NAME),
      F.col("*"))

    // get all files that are affected by the new data(update)
    val filesAreAffected = dataInTableWeShouldProcessWithFileName.join(data,
      usingColumns = idColsList,
      joinType = "inner").select(UpsertTableInDelta.FILE_NAME).
      distinct().collect().map(f => f.getString(0))

    val tmpFilePathSet = filesAreAffected.map(f => f.split("/").last).toSet

    val filesAreAffectedWithDeltaFormat = filterFiles.filter { file =>
      tmpFilePathSet.contains(file.path.split("/").last)
    }

    val deletedFiles = filesAreAffectedWithDeltaFormat.map(_.remove)

    // we should get  not changed records in affected files and write them back again
    val affectedRecords = deltaLog.createDataFrame(snapshot, filesAreAffectedWithDeltaFormat, false, None)

    val notChangedRecords = affectedRecords.join(data,
      usingColumns = idColsList, joinType = "leftanti").
      drop(F.col(UpsertTableInDelta.FILE_NAME))

    val notChangedRecordsNewFiles = txn.writeFiles(notChangedRecords, Some(options))
    val newFiles = txn.writeFiles(data, Some(options))

    notChangedRecordsNewFiles ++ newFiles ++ deletedFiles
  }

  override protected val canMergeSchema: Boolean = false
  override protected val canOverwriteSchema: Boolean = false

}

object UpsertTableInDelta {
  val ID_COLS = "idCols"
  val FILE_NAME = "__fileName__"
}


