package com.arv.com.arv.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile;

import java.io.InputStream;
import java.net.URI;

/**
 * Created by arvind on 5/25/17.
 */
public class MapFileWriteDemo {

    private static
    final String URI = "hdfs://arvind-ubuntu2:9000/user/arvind/output/hdg_ch05/mapFile";

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

        Configuration configuration = new Configuration();
        //FileSystem fileSystem = FileSystem.get(URI.create(uri),configuration);
        Path path = new Path(URI);
        IntWritable key = new IntWritable();
        Text value = new Text();
        MapFile.Writer writer = null;
        try {
            writer = new MapFile.Writer(configuration, path, SequenceFile.Writer.keyClass(key.getClass()),
                    SequenceFile.Writer.valueClass(value.getClass()), MapFile.Writer.comparator(new IntWritable.Comparator()))
            ;
            writer.setIndexInterval(10);
            for (int i = 0; i < 100; i++) {
                key.set(i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("%s\t%s\n", key, value);
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }

}
