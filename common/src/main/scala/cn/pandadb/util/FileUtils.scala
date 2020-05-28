package cn.pandadb.util

import java.io.{File, FileOutputStream, IOException, OutputStreamWriter}
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.Files

import scala.collection.JavaConversions._

object FileUtils {

  def isEmptyDirectory(directory: File): Boolean = {
    if (directory.exists) {
      if (!directory.isDirectory) throw new IllegalArgumentException("Expected directory, but was file: " + directory)
      }
    else try {
      val directoryStream = Files.newDirectoryStream(directory.toPath)
      try
        return !directoryStream.iterator.hasNext
      finally if (directoryStream != null) directoryStream.close()
    }
    true
  }

  @throws[IOException]
  def readTextFile(file: File, charset: Charset = StandardCharsets.UTF_8): String = {
    val out = new StringBuilder
    for (s <- Files.readAllLines(file.toPath, charset)) {
      out.append(s).append("\n")
    }
    out.toString
  }

  @throws[IOException]
  def writeToFile(target: File, text: String, append: Boolean): Unit = {
    if (!target.exists) {
      Files.createDirectories(target.getParentFile.toPath)
      target.createNewFile
    }
    try {
      val out = new OutputStreamWriter(new FileOutputStream(target, append), StandardCharsets.UTF_8)
      try
        out.write(text)
      finally if (out != null) out.close()
    }
  }

}
