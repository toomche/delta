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

import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.CompactTableInDelta
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._
import org.scalatest.time.SpanSugar._

// scalastyle:off: removeFile
class DeltaCompactionSuite extends StreamTest {

  override val streamingTimeout = 1800.seconds

  import testImplicits._

  private def withTempDirs(f: (File, File) => Unit): Unit = {
    withTempDir { file1 =>
      withTempDir { file2 =>
        f(file1, file2)
      }
    }
  }

  test("append mode") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>
        val inputData = MemoryStream[Int]
        val df = inputData.toDF()
        val query = df.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .format("delta")
          .start(outputDir.getCanonicalPath)
        val log = DeltaLog.forTable(spark, outputDir.getCanonicalPath)
        try {
          (1 to 15).foreach { i =>
            inputData.addData(i)
            query.processAllAvailable()
          }
          val writeThread = new Thread(new Runnable {
            override def run(): Unit =
              (1 to 15).foreach { i =>
                inputData.addData(i)
                Thread.sleep(1000)
                query.processAllAvailable()
              }
          })
          writeThread.start()
          val optimizeTableInDelta = CompactTableInDelta(log,
            new DeltaOptions(Map[String, String](), df.sparkSession.sqlContext.conf), Seq(), Map(
              CompactTableInDelta.COMPACT_VERSION_OPTION -> "8",
              CompactTableInDelta.COMPACT_NUM_FILE_PER_DIR -> "1",
              CompactTableInDelta.COMPACT_RETRY_TIMES_FOR_LOCK -> "60"
            ))
          optimizeTableInDelta.run(df.sparkSession)
          writeThread.join()
          val fileNum = new File(outputDir.getCanonicalPath).listFiles().
            filter(f => f.getName.endsWith(".parquet")).length

          val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
          assert(outputDf.count() == 30)
          assert(fileNum == 22)


        } finally {
          query.stop()
        }
      }
    }
  }
  test("append mode with partitions") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>
        val inputData = MemoryStream[A]
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
            inputData.addData(A(a, i))
            query.processAllAvailable()
          }
          val writeThread = new Thread(new Runnable {
            override def run(): Unit =
              (1 to 15).foreach { i =>
                val a = if (i > 4) "jack" else "william"
                inputData.addData(A(a, i))
                Thread.sleep(1000)
                query.processAllAvailable()
              }
          })
          writeThread.start()
          val optimizeTableInDelta = CompactTableInDelta(log,
            new DeltaOptions(Map[String, String](), df.sparkSession.sqlContext.conf), Seq(), Map(
              CompactTableInDelta.COMPACT_VERSION_OPTION -> "8",
              CompactTableInDelta.COMPACT_NUM_FILE_PER_DIR -> "1",
              CompactTableInDelta.COMPACT_RETRY_TIMES_FOR_LOCK -> "60"
            ))
          val items = optimizeTableInDelta.run(df.sparkSession)
          writeThread.join()

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
          assert(outputDf.count() == 30)
          assert(fileNum == (30 - removeFilesSize + newFilesSize))


        } finally {
          query.stop()
        }
      }
    }
  }
}

case class A(key: String, value: Int)
