package com.arv.com.arv.hdfs.java_url;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import java.io.InputStream;
import java.net.URL;

/**
 * Created by arvind on 4/4/17.
 */
public class HdfsJavaURLMain {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws Exception {
        InputStream is = new URL("hdfs://arvind-ubuntu:9000/user/arvind/input/hdg/smallfiles/a").openStream();
        int a = 0;
        while ((a = is.read()) >= 0) {
            System.out.print((char)a);
        }

    }
}
