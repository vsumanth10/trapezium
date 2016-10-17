package com.verizon.bda.trapezium.dal.lucene

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.lucene.document.{Document, StringField}
import org.apache.lucene.document._
import org.apache.lucene.search.IndexSearcher
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConverters._

class TestConverter extends SparkLuceneConverter {
  override def rowToDoc(r: Row): Document = {
    val d = new Document()
    schema.foreach {
      case StructField(name: String, StringType, _, _) => d.add(new StringField(name,
        r.getAs[String](name), Field.Store.YES))

      case StructField(name: String, ArrayType(StringType, _), _, _) =>
        r.getList[String](tldIndex).asScala.foreach { s =>
          d.add(new StringField(name, s, Field.Store.YES))
        }

      case StructField(name: String, ArrayType(IntegerType, _), _, _) =>
        r.getList[Int](indicesIndex).asScala.foreach { i =>
          d.add(new IntField(name, i, Field.Store.YES))
        }
    }

    d
  }

  lazy val tldIndex = schema.fieldIndex("tld")
  lazy val indicesIndex = schema.fieldIndex("indices")

  override def docToRow(d: Document): Row = {
    Row(d.get("id"), d.getValues("tld"), d.getFields("indices").map(_.numericValue().intValue()))
  }

  override val schema: StructType = StructType(Array(StructField("id", StringType),
    StructField("tld", ArrayType(StringType)), StructField("indices", ArrayType(IntegerType))))
}

class LuceneIndexerSuite extends FunSuite with SharedSparkContext with BeforeAndAfterAll {
  val outputPath = "target/luceneIndexerTest/"
  val hdfsIndexPath = new Path(outputPath, "hdfs").toString
  val localIndexPath = new Path(outputPath, "local").toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.registerKryoClasses(Array(classOf[IndexSearcher],
      classOf[DictionaryManager]))
    cleanup()
  }

  override def afterAll(): Unit = {
    cleanup()
    super.afterAll()
  }

  private def cleanup(): Unit = {
    val f = new File(outputPath)
    if (f.exists()) {
      FileUtils.deleteQuietly(f)
    }
  }

  test("IndexTest") {

    val numShards = 2

    val idx = new LuceneIndexer("local", localIndexPath, hdfsIndexPath, "tld")
      .setConverter(new TestConverter())
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.createDataFrame(Seq(("123", Array("verizon.com", "google.com"),
      Array[Int](5, 10, 17, 29)),
      ("456", Array("apple.com", "google.com"),
        Array[Int](6, 10, 14, 22))))
      .toDF("id", "tld", "indices")
    idx.index(df, numShards)
    idx.loadIndex(sc, numShards)

    assert(idx.search("tld:google.com").count == 2)
    assert(idx.search("tld:verizon.com").count == 1)
  }
}
