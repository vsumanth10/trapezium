package com.verizon.bda.trapezium.dal.lucene

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by pramod.lakshminarasimha on 8/10/16.
  */
class DictionaryManagerSuite extends FunSuite with SharedSparkContext with BeforeAndAfterAll {

  val outputPath = "target/dictionary"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val f = new File(outputPath)
    if(f.exists()) {
      FileUtils.deleteQuietly(f)
    }
  }

  override def afterAll(): Unit = {
    val f = new File(outputPath)
    if(f.exists()) {
      FileUtils.deleteQuietly(f)
    }
    super.afterAll()
  }

  lazy val sqlContext = SQLContext.getOrCreate(sc)

  lazy val df: DataFrame = sqlContext.createDataFrame(
    sc.parallelize(Seq(Row("s1", Array("tld1", "tld2"), Array("zip1", "zip2")),
    Row("s2", Array("tld1", "tld3"), Array("zip3", "zip4")),
    Row("s3", Array("tld2", "tld4"), Array("zip5", "zip2")),
    Row("s4", Array("tld4", "tld3"), Array("zip6", "zip4")))),
    StructType(Array(StructField("subid", StringType),
      StructField("tlds", ArrayType(StringType)),
      StructField("zips", ArrayType(StringType)))))

  test("Dictionary manager functional test") {
    val dm = new DictionaryManager

    Seq("tlds", "zips").foreach(f =>
      dm.addToDictionary(f, df.select(explode(df(f))).rdd.map(_.getAs[String](0))))

    checkAsserts(dm)
  }

  test("Dictionary manager io test") {
    val dm = new DictionaryManager

    Seq("tlds", "zips").foreach(f =>
      dm.addToDictionary(f, df.select(explode(df(f))).rdd.map(_.getAs[String](0))))

    dm.save("target/dictionary")(sc)

    val dm2 = new DictionaryManager
    dm2.load("target/dictionary")(sc)

    checkAsserts(dm2)
  }

  private def checkAsserts(dm: DictionaryManager): Unit = {
    assert(dm.indexOf("tlds", "tld1") != -1)
    assert(dm.indexOf("tlds", "tld2") != -1)
    assert(dm.indexOf("tlds", "tld3") != -1)
    assert(dm.indexOf("tlds", "tld4") != -1)
    assert(dm.indexOf("tlds", "tld5") == -1)
    assert(dm.indexOf("zips", "zip1") != -1)
    assert(dm.indexOf("zips", "zip2") != -1)
    assert(dm.indexOf("zips", "zip7") == -1)

    val tlds = Set("tld1", "tld2", "tld3", "tld4")
    assert(tlds.contains(dm.getFeatureName(0)))
    assert(tlds.contains(dm.getFeatureName(1)))
    assert(tlds.contains(dm.getFeatureName(2)))
    assert(tlds.contains(dm.getFeatureName(3)))

    val zips = Set("zip1", "zip2", "zip3", "zip4", "zip5", "zip6")
    assert(zips.contains(dm.getFeatureName(4)))
    assert(zips.contains(dm.getFeatureName(5)))
    assert(zips.contains(dm.getFeatureName(6)))
    assert(zips.contains(dm.getFeatureName(7)))
    assert(zips.contains(dm.getFeatureName(8)))
    assert(zips.contains(dm.getFeatureName(9)))

    assert(dm.size() == 10)

    assert(dm.getFeatureType(0) == "tlds")
    assert(dm.getFeatureType(1) == "tlds")
    assert(dm.getFeatureType(2) == "tlds")
    assert(dm.getFeatureType(3) == "tlds")
    assert(dm.getFeatureType(4) == "zips")
    assert(dm.getFeatureType(5) == "zips")
    assert(dm.getFeatureType(6) == "zips")
    assert(dm.getFeatureType(7) == "zips")
    assert(dm.getFeatureType(8) == "zips")
    assert(dm.getFeatureType(9) == "zips")

    assert(dm.getRange("tlds") == (0, 3))
    assert(dm.getRange("zips") == (4, 9))
  }
}
