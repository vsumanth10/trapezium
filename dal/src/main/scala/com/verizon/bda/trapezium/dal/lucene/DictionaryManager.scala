package com.verizon.bda.trapezium.dal.lucene

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer => MArray, Map => MMap}

/**
  * Class to manage dictionaries for multiple dimensions.
  *
  * @author pramod.lakshminarasimha on 8/10/16.
  */
class DictionaryManager extends Serializable {

  case class FeatureAttr(dictionaryPos: Int, featureOffset: Int) extends Serializable

  /**
    * offset points to the last index of the aggregated features
    * array in {{{featureNameLookup}}}
    */
  private var offset: Int = 0
  /**
    * namesMap keeps track of the position of a particular dictionary
    * in the array of {{{dictionaries}}}
    */
  private var namesMap: MMap[String, FeatureAttr] = MMap[String, FeatureAttr]()
  private var dictionaries: MArray[Map[String, Int]] = MArray[Map[String, Int]]()
  private val featureNameLookup: MArray[String] = MArray[String]()

  /**
    * Function to create index for a dimension and append to current dictionary.
    *
    * @param name Name of the dimension
    * @param rdd  RDD containing values for the {{{name}}} dimension
    */
  def addToDictionary(name: String, rdd: RDD[String]): Unit = {

    require(!dictionaries.contains(name), s"Dictionary already created for ${name}")

    val dictionary: Map[String, Int] = rdd.distinct()
      .zipWithIndex
      .map(kv => (kv._1, kv._2.toInt + offset))
      .collect().toMap

    dictionaries.append(dictionary)
    namesMap.put(name, FeatureAttr(dictionaries.size - 1, offset))
    updateFeatureNameLookup(dictionary)
  }

  /**
    * Total number of keys in the dictionary.
    *
    * @return Int size of the dictionary.
    */
  def size(): Int = offset

  /**
    * Get feature value given the index.
    *
    * @param i index of the feature
    * @return value of the feature
    */
  def getFeatureName(i: Int): String = {

    require(featureNameLookup.size > i,
      s"Index value not found. Current size: ${featureNameLookup.size}. Looking up: $i.")

    featureNameLookup(i)
  }

  /**
    * Get feature type given an index.
    *
    * @param i index of the feature to find the type.
    * @return feature type of feature at index i
    */
  def getFeatureType(i: Int): String = {

    require(featureNameLookup.size > i,
      s"Index value not found. Current size: ${featureNameLookup.size}. Looking up: $i.")

    val featureType = namesMap.keys.filter {
      n: String => {
        val range = getRange(n)
        range._1 <= i && range._2 >= i
      }
    }

    assert(featureType.size == 1,
      s"Error in Dictionary Manager. Index $i matched ${featureType.mkString(",")}")

    featureType.head
  }

  /**
    * Given the dimension and feature value, return it's index.
    *
    * @param name    Name of the dimension.
    * @param feature Feature name for which the index is fetched.
    * @return Int index of the feature name in the dictionary.
    */
  def indexOf(name: String, feature: String): Int = {
    assert(namesMap.contains(name), s"Dictionary not found for $name.")
    dictionaries(namesMap.get(name).get.dictionaryPos).getOrElse(feature, -1)
  }

  /**
    * Given the dimension, return the start and end positions of the indices.
    *
    * @param name name of the dimension.
    * @return start and end position tuple.
    */
  def getRange(name: String): (Int, Int) = {

    assert(namesMap.contains(name), s"Dictionary not found for $name.")

    val attr = namesMap.get(name).get
    (attr.featureOffset, attr.featureOffset + dictionaries(attr.dictionaryPos).size - 1)
  }

  def save(path: String)(implicit sc: SparkContext): Unit = {
    val paths = getDictionaryPaths(path)
    sc.parallelize(Seq(dictionaries), 1).saveAsObjectFile(paths._1)
    sc.parallelize(Seq(namesMap), 1).saveAsObjectFile(paths._2)
  }

  def load(path: String)(implicit sc: SparkContext): Unit = {
    val paths = getDictionaryPaths(path)
    dictionaries = sc.objectFile(paths._1).map {
      x: Any => x.asInstanceOf[MArray[Map[String, Int]]]
    }.collect()(0)
    namesMap = sc.objectFile(paths._2).map {
      x: Any => x.asInstanceOf[MMap[String, FeatureAttr]]
    }.collect()(0)
    offset = 0
    dictionaries.foreach((d: Map[String, Int]) => updateFeatureNameLookup(d))
  }

  private def getDictionaryPaths(path: String): (String, String) = {
    ((new Path(path, "dictionaryMaps")).toString, (new Path(path, "namesMap")).toString)
  }

  private def updateFeatureNameLookup(dictionary: Map[String, Int]) {
    val a: Array[String] = Array.fill[String](dictionary.size)("")

    dictionary.foreach((x: (String, Int)) => {
      a((x._2 - offset).toInt) = x._1
    })

    featureNameLookup.appendAll(a)
    offset = featureNameLookup.size
  }

  override def toString: String = {
    s"""Offset: $offset
        |Names map: $namesMap
        |Dictionaries: $dictionaries
        |featureNames: $featureNameLookup""".stripMargin
  }
}
