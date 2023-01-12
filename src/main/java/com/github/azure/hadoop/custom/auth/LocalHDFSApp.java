package com.github.azure.hadoop.custom.auth;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.Date;

public class LocalHDFSApp {
    public static void main(String[] args) {
        String localFileSystemFile = "./tmp/";
        // Get configuration of Hadoop system
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            Path path= new Path(localFileSystemFile);
            for(int i=0;i<3;i++) {
                writeToFile(fs, path);
                Thread.sleep(1000);
            }
            readFiles(fs, path);


        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }

    public static void readFiles(FileSystem fs, Path path) throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, false);

        if (files != null) {
            while(files.hasNext()) {
                LocatedFileStatus file = files.next();

                    try{
                        String str = IOUtils.toString(fs.open(file.getPath()));
                        System.out.println("File path"+file.getPath().toString()+"File content: " + str);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw e;
                    } finally {
                        fs.close();
                    }


            }
        }
    }

    public static void writeToFile(FileSystem fs, Path path) {
        try{
            FSDataOutputStream out = null;
            try {
                if (!fs.exists(path)) {
                    fs.mkdirs(path);
                }
                String content = String.valueOf((new Date()).getTime());
                Path filepath = new Path(path+ "/"+content+".txt");
                out = fs.create(filepath, true);
                out.write(content.getBytes());
                out.flush();
                System.out.println("File written to HDFS "+filepath.toString()+" with content "+content);
            } catch (IOException e) {
                throw e;
            } finally {
                if(out!=null) {
                    out.close();
                }

            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
