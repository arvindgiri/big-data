package com.arv.com.arv.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.InputStream;
import java.net.URI;

/**
 * Created by arvind on 5/25/17.
 */
public class SequenceFileReadDemo {

    public static void main(String[] args) throws Exception {
        String uri = "hdfs://arvind-ubuntu2:9000/user/arvind/output/hdg_ch05/sequenceFile";
        Configuration configuration = new Configuration();
        //FileSystem fileSystem = FileSystem.get(URI.create(uri),configuration);
        Path path = new Path(uri);
        Writable key;
        Writable value;
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(configuration, Reader.file(path));
            key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
            value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition();
            }
        } finally {
            IOUtils.closeStream(reader);
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
