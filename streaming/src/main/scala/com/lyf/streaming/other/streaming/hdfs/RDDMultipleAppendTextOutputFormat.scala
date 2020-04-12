package com.lyf.streaming.other.streaming.hdfs

import java.io.DataOutputStream

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, RecordWriter, TextOutputFormat}
import org.apache.hadoop.mapred.lib.MultipleOutputFormat
import org.apache.hadoop.util.{Progressable, ReflectionUtils}


/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/12 11:39
  * Version: 1.0
  */
class RDDMultipleAppendTextOutputFormat extends MultipleOutputFormat[Any, Any]{
  private var theTextOutputFormat: AppendTextOutputFormat = null
  //追加写
  override def getBaseRecordWriter(fileSystem: FileSystem, jobConf: JobConf, s: String, progressable: Progressable): RecordWriter[Any,Any] = {
      if(this.theTextOutputFormat == null){
        this.theTextOutputFormat = new AppendTextOutputFormat
      }
      this.theTextOutputFormat.getRecordWriter(fileSystem,jobConf,s,progressable)
  }

  override def generateActualKey(key: Any, value: Any): Any = {
    NullWritable.get()
  }
}

/**
 *  @author lyf3312
 *  @date 20/04/12 12:46
 *  @description 自定义outputFormat
 *
 */
class AppendTextOutputFormat extends TextOutputFormat[Any, Any]{
  override def getRecordWriter(ignored: FileSystem, job: JobConf, iname: String, progress: Progressable): RecordWriter[Any, Any] = {
    val isCompressed = FileOutputFormat.getCompressOutput(job)
    val keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator", "\t")
    //自定义输出文件名
    val name = job.get("filename", iname)
    if (!isCompressed) {
      val file: Path = FileOutputFormat.getTaskOutputPath(job, name)
      val fs: FileSystem = file.getFileSystem(job)
      val newFile: Path = new Path(FileOutputFormat.getOutputPath(job), name)
      val fileOut: FSDataOutputStream = if (fs.exists(newFile)) {
        //存在，追加写
        fs.append(newFile)
      } else {
        fs.create(file, progress)
      }
      new TextOutputFormat.LineRecordWriter[Any, Any](fileOut, keyValueSeparator)
    } else {
      val codecClass: Class[_ <: CompressionCodec] = FileOutputFormat.getOutputCompressorClass(job, classOf[GzipCodec])
      // create the named codec
      val codec: CompressionCodec = ReflectionUtils.newInstance(codecClass, job)
      // build the filename including the extension
      val file: Path = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension)
      val fs: FileSystem = file.getFileSystem(job)
      val newFile: Path = new Path(FileOutputFormat.getOutputPath(job), name + codec.getDefaultExtension)

      val fileOut: FSDataOutputStream = if (fs.exists(newFile)) {
        //存在，追加写
        fs.append(newFile)
      } else {
        fs.create(file, progress)
      }
      new TextOutputFormat.LineRecordWriter[Any, Any](new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator)
    }
  }
}
