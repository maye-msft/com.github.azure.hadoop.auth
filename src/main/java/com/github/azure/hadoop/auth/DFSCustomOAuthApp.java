package com.github.azure.hadoop.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

public class DFSCustomOAuthApp {

    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();
        conf.set("fs.azure.account.auth.type", "Custom");
        conf.set("fs.azure.account.oauth.provider.type", "com.github.azure.hadoop.auth.OAuthBasedAccessTokenProvider");
        conf.set("fs.azure.account.oauth2.client.endpoint", args[1]);
        conf.set("fs.azure.account.oauth2.client.id", args[2]);
        conf.set("fs.azure.account.oauth2.client.secret", args[3]);

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
