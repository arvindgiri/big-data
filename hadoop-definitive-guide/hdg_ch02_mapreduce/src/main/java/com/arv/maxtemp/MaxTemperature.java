package com.arv.maxtemp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/**
 * Created by arvind on 19/3/17.
 */
public class MaxTemperature {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        String inputPath = "/user/arvind/input/hdg/ncdc/all";
        String outputPath = "/user/arvind/output/maxtemp";
        if (args.length > 0) {
            inputPath = args[0];
        }

        if (args.length > 1) {
            outputPath = args[1];
        }

        Locale.setDefault(Locale.US);
        Logger
                .getLogger("org.apache.hadoop").setLevel(Level.ALL);
        ConsoleAppender appender = new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN));
        appender.setName("arv_Console");
        Logger
                .getRootLogger().addAppender(appender);
        Job job = Job.getInstance();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Arvind test job 1");
        FileSystem.get(new URI("/"), new Configuration()).delete(new Path(outputPath), true);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.submit();
        job.waitForCompletion(true);
        System.out.println(job.getWorkingDirectory());
        System.out.println(job.getCounters());
    }
}
