package com.arv.maxtemp;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by arvind on 7/3/17.
 */


class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = 0;
        for (IntWritable value : values) {
            if (max < value.get())
                max = value.get();
        }
        context.write(key, new IntWritable(max));
    }

}