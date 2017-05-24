package com.arv.com.arv.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

import java.io.InputStream;
import java.net.URI;

/**
 * Created by arvind on 5/25/17.
 */
public class SequenceFileWriteDemo {
    private static final String[] DATA = {
            "One, two,",
            "Buckle my shoe;",
            "Three, four,",
            "Knock at the door;",
            "Five, six,",
            "Pick up sticks;",
            "Seven, eight,",
            "Lay them straight:",
            "Nine, ten,",
            "A big fat hen;",
            "Eleven, twelve,",
            "Dig and delve;",
            "Thirteen, fourteen,",
            "Maids a-courting;",
            "Fifteen, sixteen,",
            "Maids in the kitchen;",
            "Seventeen, eighteen,",
            "Maids in waiting",
            "Nineteen, twenty,",
            "My plate's empty"
    };


    public static void main(String[] args) throws Exception {
        String uri = "hdfs://arvind-ubuntu2:9000/user/arvind/output/hdg_ch05/sequenceFile";
        Configuration configuration = new Configuration();
        //FileSystem fileSystem = FileSystem.get(URI.create(uri),configuration);
        Path path = new Path(uri);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(configuration, Writer.file(path), Writer.keyClass(key.getClass()), Writer.valueClass(value.getClass()));
            for (int i = 0; i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    public static void main_old(String[] args) throws Exception {
        String uri = "hdfs://arvind-ubuntu:9000/user/arvind/input/hdg/smallfiles/a";
        if (args.length > 0) {
            uri = args[0];
        }
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(uri), conf);
        InputStream in = null;

        try {
            in = fileSystem.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4094, false);
        } catch (Exception e) {
            IOUtils.closeStream(in);
        }
    }
}
