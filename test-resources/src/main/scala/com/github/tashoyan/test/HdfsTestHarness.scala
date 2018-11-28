package com.github.tashoyan.test

import java.io.{File, IOException}
import java.net.{URI, URL}
import java.nio.file.Paths

import com.github.tashoyan.test.HdfsTestHarness._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, RemoteIterator}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.log4j.{Level, Logger}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.io.Source
import scala.language.implicitConversions

abstract class HdfsTestHarness extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  protected val hdfsBaseDir: File = Paths.get(sys.props("user.dir"), "target", "hdfs").toFile
  protected var hdfsCluster: MiniDFSCluster = _
  protected var hdfsUri: URI = _
  protected def fs: FileSystem = hdfsCluster.getFileSystem

  override protected def beforeAll(): Unit = {
    suppressHadoopInfoLogging()
    setupHdfs()
  }

  override protected def afterAll(): Unit = {
    hdfsCluster.shutdown(true)
  }

  override protected def beforeEach(): Unit = {
    if (!cleanup())
      throw new AssertionError("Failed to cleanup")
  }

  protected def suppressHadoopInfoLogging(): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("BlockStateChange").setLevel(Level.WARN)
    Logger.getLogger("org.mortbay").setLevel(Level.WARN)
  }

  protected def setupHdfs(): Unit = {
    FileUtil.fullyDelete(hdfsBaseDir)
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsBaseDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(conf)
    hdfsCluster = builder.build()
    hdfsUri = fs.getUri
  }

  /**
    * Cleanup the filesystem.
    * Equivalent to `rm -r /`.
    *
    * @return true if the file system became clean.
    */
  protected def cleanup(): Boolean = {
    fs.listStatus("/")
      .foreach(status =>
        fs.delete(status.getPath, true))
    fs.listStatus("/").isEmpty
  }

  /**
    * Checks if it is a directory.
    *
    * @param target Target to check.
    * @return true if the target is a directory.
    */
  protected def isDir(target: String): Boolean = {
    fs.getFileStatus(target).isDirectory
  }

  /**
    * Checks if it is a file.
    *
    * @param target Target to check.
    * @return true if the target is a file.
    */
  protected def isFile(target: String): Boolean = {
    fs.getFileStatus(target).isFile
  }

  /**
    * Makes directories recursively.
    * Equivalent to `mkdir -p`.
    *
    * @param target Directory to create.
    * @return true if the directory is created successfully.
    */
  protected def mkdirs(target: String): Boolean = {
    fs.mkdirs(target)
  }

  /**
    * Creates an empty file if not existed yet, otherwise updates file attributes.
    * Equivalent to `touch` command.
    *
    * @param target File to touch.
    */
  protected def touch(target: String): Unit = {
    val output = fs.create(target)
    output.close()
  }

  /**
    * Put a resource to a directory on HDFS.
    *
    * @param src    Resource to put.
    * @param target Target directory on HDFS.
    */
  protected def put(src: URL, target: String): Unit = {
    if (!isDir(target)) {
      throw new IOException(s"Not a HDFS directory: $target")
    }
    fs.copyFromLocalFile(false, true, src, target)
  }

  /**
    * List content of a directory.
    *
    * @param dir Directory to list.
    * @return Collection of files and directories within the given directory.
    */
  protected def ls(dir: String): Seq[String] = {
    if (!isDir(dir)) {
      throw new IOException(s"Not a HDFS directory: $dir")
    }
    fs.listStatus(dir)
      .map(_.getPath
        .getName)
  }

  /**
    * Read content of a file.
    *
    * @param fileName File to read.
    * @return Content of the file.
    */
  protected def read(fileName: String): Source = {
    if (!isFile(fileName)) {
      throw new IOException(s"Not a HDFS file: $fileName")
    }
    val inputStream = fs.open(fileName)
    Source.fromInputStream(inputStream)
  }
}

object HdfsTestHarness {

  implicit def stringPathToHdfsPath(path: String): Path = new Path(path)

  implicit def uriToHdfsPath(uri: URI): Path = new Path(uri)

  implicit def urlToHdfsPath(url: URL): Path = new Path(url.toURI)

  implicit def remoteIteratorToIterator[E](iterator: RemoteIterator[E]): Iterator[E] =
    new Iterator[E] {
      override def hasNext: Boolean = iterator.hasNext

      override def next(): E = iterator.next()
    }
}
