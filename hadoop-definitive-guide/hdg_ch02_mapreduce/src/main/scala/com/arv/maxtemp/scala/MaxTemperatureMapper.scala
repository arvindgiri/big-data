package com.arv.maxtemp.scala

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/**
  * Created by arvind on 7/3/17.
  */

//class MaxTemperatureMapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Mapper {
//  override def map(key: Nothing, value: Nothing, context: Mapper[Nothing, Nothing, Nothing, Nothing]#Context): Unit = {
//    print(key)
//  }
//}


class MaxTemperatureMapperScala extends Mapper[Object, Text, Text, IntWritable] {
  private final val MISSING: Int = 9999;

  override
  def map(key: Object, value: Text,
          context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val line = value.toString
    val year = line.substring(15, 19)
    var airTemperature = 0
    if (line.charAt(87) == '+')
      airTemperature = Integer.parseInt(line.substring(88, 92))
    else
      airTemperature = Integer.parseInt(line.substring(87, 92))

    val quality = line.substring(92, 93)
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      context.write(new Text(year), new IntWritable(airTemperature))
    }
  }
}