package com.arv.emp.secondarysort;

import org.apache.hadoop.mapreduce.Job;

/**
 * Created by arvind on 21/3/17.
 */
public class EmpSsDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("Arv Emp Secondary sort");
        job.setJarByClass(EmpSsDriver.class);
//
//        FileInputFormat.addInputPath(new Path("src/main/resources/emp"));
//        FileOutputFormat.setOutputPath(new Path("output"));


    }
}
