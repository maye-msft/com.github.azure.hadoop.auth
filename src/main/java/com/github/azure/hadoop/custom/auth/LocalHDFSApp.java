package com.github.azure.hadoop.custom.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class LocalHDFSApp {
    public static void main(String[] args) {
        String localFileSystemFile = "./tmp/tmp1/test.txt";
        String destinationPath = "./tmp/tmp2";

        // Get configuration of Hadoop system
        Configuration conf = new Configuration();
        conf.addResource(new Path(
                "C:\\devtools\\hadoop-3.3.1\\etc\\hadoop\\core-site.xml"));
        conf.addResource(new Path(
                "C:\\devtools\\hadoop-3.3.1\\etc\\hadoop\\hdfs-site.xml"));
        conf.addResource(new Path(
                "C:\\devtools\\hadoop-3.3.1\\etc\\hadoop\\mapred-site.xml"));
        try{
            Path srcPath = new Path(localFileSystemFile);
            FileSystem fileSystem = FileSystem.get(conf);


            Path dstPath = new Path(destinationPath);
            // Check if the file already exists
            if (!(fileSystem.exists(dstPath))) {
                System.out.println("No such destination " + dstPath);
                return;
            }

            fileSystem.copyFromLocalFile(srcPath, dstPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }



    }
}
