package com.github.azure.hadoop.custom.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

public class DFSCustomHDFSCacheOAuthApp {

    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();
        conf.set("fs.azure.account.auth.type", "Custom");
        conf.set("fs.azure.account.oauth.provider.type", "com.github.azure.hadoop.custom.auth.OAuthHDFSCachedAccessTokenProvider");
        conf.set("fs.azure.account.oauth2.client.endpoint", args[1]);
        conf.set("fs.azure.account.oauth2.client.id", args[2]);
        conf.set("fs.azure.account.oauth2.client.secret", args[3]);
        conf.set("fs.azure.custom.token.fetch.retry.count", "3");
//        conf.set("fs.azure.custom.token.hdfs.cache.path", args[4]);
        conf.set("fs.azure.custom.token.cache.delete.on.exit", "false");


        //org.apache.log4j.BasicConfigurator.configure();

        PatternLayout layout = new PatternLayout();
        String conversionPattern = "%-7p %d [%t] %c %x - %m%n";
        layout.setConversionPattern(conversionPattern);

        // creates console appender
        ConsoleAppender consoleAppender = new ConsoleAppender();
        consoleAppender.setLayout(layout);
        consoleAppender.activateOptions();


        // configures the root logger
        Logger logger = Logger.getLogger("com.github.azure.hadoop.custom.auth.HDFSCachedAccessTokenProvider");
        logger.setLevel(org.apache.log4j.Level.DEBUG);
        logger.addAppender(consoleAppender);


        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
        System.out.println();
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }


    }
}
