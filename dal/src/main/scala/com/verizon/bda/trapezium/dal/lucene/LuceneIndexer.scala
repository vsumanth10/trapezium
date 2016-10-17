package com.verizon.bda.trapezium.dal.lucene

import java.io.{File, IOException}
import java.nio.file.FileSystems

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{BooleanQuery, IndexSearcher}
import org.apache.lucene.store.MMapDirectory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType

// noinspection ScalaStyle


class LuceneIndexer(env: String, val indexPathInput: String,
                    val hdfsPath: String,
                    defaultSearchField: String,
                    maxRowsPerPartition: Int = Integer.MAX_VALUE) extends Serializable {

  @transient lazy val log = Logger.getLogger(classOf[LuceneIndexer])

  val indexPath = if (indexPathInput.endsWith(File.separator)) {
    indexPathInput
  }
  else {
    indexPathInput + File.separator
  }

  lazy val analyzer = new KeywordAnalyzer()
  var shards: RDD[IndexSearcher] = _
  var converter: SparkLuceneConverter = _

  val PREFIX = "part-"

  def setConverter(converter: SparkLuceneConverter): this.type = {
    this.converter = converter
    this
  }

  def index(dataframe: DataFrame, numShards: Int): Unit = {
    val conf = new Configuration
    FileSystem.get(conf).delete(new Path(hdfsPath), true)


    dataframe.coalesce(numShards).rdd.mapPartitionsWithIndex((i, itr) => {
      val dir = new File(indexPath + PREFIX + i + File.separator)
      FileUtils.forceDeleteOnExit(dir)
      if (dir.isDirectory()) {
        FileUtils.cleanDirectory(dir)
      }


      val indexWriterConfig = new IndexWriterConfig(analyzer)
      indexWriterConfig.setOpenMode(OpenMode.CREATE)
      val localPath = FileSystems.getDefault().getPath(indexPath, PREFIX + i)
      val directory = new MMapDirectory(localPath)

      val indexWriter = new IndexWriter(directory, indexWriterConfig)
      itr.foreach {
        r => {
          try {
            val d = converter.rowToDoc(r)
            indexWriter.addDocument(d);
          } catch {
            case e: Throwable => {
              log.error(s"Error with row: ${r}")
              throw new RuntimeException(e)
            }
          }
        }
      }
      indexWriter.commit()
      log.debug("Number of documents indexed in this partition: " + indexWriter.maxDoc())
      indexWriter.close
      val conf = new Configuration
      if (env != "local") {
        FileSystem.get(conf).copyFromLocalFile(true, true,
          new Path(indexPath, PREFIX + i), new Path(hdfsPath))
      }
      else {
        FileSystem.get(conf).copyFromLocalFile(true, true,
          new Path(indexPath, PREFIX + i), new Path(hdfsPath, PREFIX + i))
      }
      Iterator.empty
    }).count()

    FileSystem.closeAll()

    val filesList = FileSystem.get(conf).listFiles(new Path(hdfsPath), true)
    while (filesList.hasNext())
      log.debug(filesList.next().getPath.toString())

    log.info("Number of partitions: " + dataframe.rdd.getNumPartitions)
  }

  def loadIndex(sc: SparkContext, numShards: Int): Unit = {

    shards = sc.parallelize((0 until numShards).toList, sc.defaultParallelism)
      .flatMap { i: Int => {
        val dir = new File(indexPath + PREFIX + i + File.separator)
        FileUtils.forceDeleteOnExit(dir)
        if (dir.isDirectory()) {
          FileUtils.cleanDirectory(dir)
        }
        val conf = new Configuration
        val searcher: Option[IndexSearcher] = {
          log.debug("Copying data from hdfs: " + hdfsPath + PREFIX + i
            + " to local: " + indexPath + PREFIX + i + File.separator)
          try {
            FileSystem.get(conf).copyToLocalFile(false,
              new Path(hdfsPath, PREFIX + i + File.separator),
              new Path(indexPath + PREFIX + i + File.separator + "CONTENT" + File.separator))
            val reader = DirectoryReader.open(
              new MMapDirectory(FileSystems.getDefault()
                .getPath(indexPath, PREFIX + i, "CONTENT")))
            Some(new IndexSearcher(reader))
          } catch {
            case e: IOException => None
            case x: Throwable => throw new RuntimeException(x)
          }
        }
        searcher
      }
      }
    shards.cache()
    log.info("Number of shards: " + shards.count())
  }

  lazy val qp = new QueryParser(defaultSearchField, analyzer)

  def search(queryStr: String): RDD[Row] = {

    val rows: RDD[Row] = shards.flatMap { searcher: IndexSearcher => {
      BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
      val topDocs = searcher.search(qp.parse(queryStr), maxRowsPerPartition)

      log.debug("Hits within partition: " + topDocs.totalHits)
      topDocs.scoreDocs.map { d => converter.docToRow(searcher.doc(d.doc)) }
    }
    }

    rows
  }

  def count(queryStr: String): Int = {
    shards.map { searcher: IndexSearcher => {
      searcher.count(qp.parse(queryStr))
    }
    }.sum().toInt
  }
}

trait SparkLuceneConverter extends Serializable {

  def rowToDoc(r: Row): Document

  def docToRow(d: Document): Row

  val schema: StructType

}
