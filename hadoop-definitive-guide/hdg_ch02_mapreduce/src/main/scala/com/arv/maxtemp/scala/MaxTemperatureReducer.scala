package com.arv.maxtemp.scala

import java.lang.Iterable

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

/**
  * Created by arvind on 7/3/17.
  */


class MaxTemperatureReducerScala extends Reducer[Text, IntWritable, Text, IntWritable] {

  override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    var max = 0;
    while (values.iterator().hasNext) {
      val value: IntWritable = values.iterator().next()
      max = if (max > value.get()) max else value.get()
    }
    context.write(key, new IntWritable(max))
  }

}