/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import java.io.File

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.UpsertTableInDelta
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._
import org.scalatest.time.SpanSugar._

// scalastyle:off: removeFile
class DeltaUpsertSuite extends StreamTest {

  override val streamingTimeout = 1800.seconds

  import testImplicits._

  private def withTempDirs(f: (File, File) => Unit): Unit = {
    withTempDir { file1 =>
      withTempDir { file2 =>
        f(file1, file2)
      }
    }
  }

  test("upsert stream table") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>
        val inputData = MemoryStream[A2]
        val df = inputData.toDF()
        val query = df.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .format("delta")
          .partitionBy("key")
          .start(outputDir.getCanonicalPath)
        val log = DeltaLog.forTable(spark, outputDir.getCanonicalPath)
        try {
          (1 to 15).foreach { i =>
            val a = if (i > 3) "jack" else "william"
            inputData.addData(A2(a, i, i + 1))
            query.processAllAvailable()
          }

          val data = Seq(A2("william", 1, 2), A2("william", 16, 2)).toDF()

          val upsertTableInDelta = UpsertTableInDelta(data, Option(SaveMode.Append), None, log,
            new DeltaOptions(Map[String, String](), df.sparkSession.sessionState.conf),
            Seq(),
            Map("idCols" -> "key,value"))
          val items = upsertTableInDelta.run(df.sparkSession)

          def recursiveListFiles(f: File): Array[File] = {
            val these = f.listFiles
            these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
          }


          val fileNum = recursiveListFiles(new File(outputDir.getCanonicalPath)).filter { f =>
            f.getName.endsWith(".parquet") && !f.getName.contains("checkpoint")
          }.length


          val acitons = items.map(f => Action.fromJson(f.getString(0)))
          val newFilesSize = acitons.filter(f => f.isInstanceOf[AddFile]).size
          val removeFilesSize = acitons.filter(f => f.isInstanceOf[RemoveFile]).size

          val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
          assert(outputDf.count() == 16)
          assert(fileNum == (15 + newFilesSize)) notifyAll()


        } finally {
          query.stop()
        }
      }
    }
  }

  test("upsert stream table without partition") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>
        val inputData = MemoryStream[A2]
        val df = inputData.toDF()
        val query = df.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .format("delta")
          .start(outputDir.getCanonicalPath)
        val log = DeltaLog.forTable(spark, outputDir.getCanonicalPath)
        try {
          (1 to 15).foreach { i =>
            val a = if (i > 3) "jack" else "william"
            inputData.addData(A2(a, i, i + 1))
            query.processAllAvailable()
          }

          val data = Seq(A2("william", 1, 2), A2("william", 16, 2)).toDF()

          val upsertTableInDelta = UpsertTableInDelta(data, Option(SaveMode.Append), None, log,
            new DeltaOptions(Map[String, String](), df.sparkSession.sessionState.conf),
            Seq(),
            Map("idCols" -> "key,value"))
          val items = upsertTableInDelta.run(df.sparkSession)

          def recursiveListFiles(f: File): Array[File] = {
            val these = f.listFiles
            these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
          }


          val fileNum = recursiveListFiles(new File(outputDir.getCanonicalPath)).filter { f =>
            f.getName.endsWith(".parquet") && !f.getName.contains("checkpoint")
          }.length


          val acitons = items.map(f => Action.fromJson(f.getString(0)))
          val newFilesSize = acitons.filter(f => f.isInstanceOf[AddFile]).size
          val removeFilesSize = acitons.filter(f => f.isInstanceOf[RemoveFile]).size

          val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
          assert(outputDf.count() == 16)
          assert(fileNum == (15 + newFilesSize))


        } finally {
          query.stop()
        }
      }
    }
  }
}

case class A2(key: String, value: Int, value2: Int)
