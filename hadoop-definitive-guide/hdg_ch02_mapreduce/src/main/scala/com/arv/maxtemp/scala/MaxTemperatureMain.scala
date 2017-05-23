package com.arv.maxtemp.scala

import java.net.URI
import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.log4j.{ConsoleAppender, Level, Logger, PatternLayout}


/**
  * Hello world!
  *
  */

// Setup local debugging: http://vichargrave.com/debugging-hadoop-applications-with-intellij/
// Setup local debugging: http://vichargrave.com/create-a-hadoop-build-and-development-environment-for-hadoop/#get-and-build-hadoop-from-trunk

// Debug on eclipse
// http://www.thecloudavenue.com/2012/10/debugging-hadoop-mapreduce-program-in.html


object MaxTemperatureScala extends App {
  Locale.setDefault(Locale.US)
  Logger
    .getLogger("org.apache.hadoop").setLevel(Level.ALL);
  val job = Job.getInstance();
  var appender: ConsoleAppender = new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN));
  appender.setName("arv_Console")
  Logger
    .getRootLogger.addAppender(appender)
  //  job.getConfiguration.set("mapreduce.framework.name","local")
  job.setJarByClass(MaxTemperatureScala.getClass)
  job.setJobName("Arv Max Temp")
  //FileInputFormat.addInputPath(job, new Path("src/main/resources/all"));
  FileInputFormat.addInputPath(job, new Path("src/main/resources/sample.txt"));
  FileOutputFormat.setOutputPath(job, new Path("output"));
  FileSystem.get(URI.create("output"), new Configuration()).delete(new Path("output"), true);

  job.setMapperClass(classOf[MaxTemperatureMapperScala])
  job.setReducerClass(classOf[MaxTemperatureReducerScala])
  job.setMapOutputKeyClass(classOf[Text])
  job.setMapOutputValueClass(classOf[IntWritable])
  job.submit();
  print(job.getTrackingURL)
  job.waitForCompletion(true);
}
