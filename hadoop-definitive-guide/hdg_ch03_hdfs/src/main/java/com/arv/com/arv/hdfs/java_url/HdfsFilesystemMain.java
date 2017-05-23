package com.arv.com.arv.hdfs.java_url;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

/**
 * Created by arvind on 4/4/17.
 */
public class HdfsFilesystemMain {
    public static void main(String[] args) throws Exception {
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
