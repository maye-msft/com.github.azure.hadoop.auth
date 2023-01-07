package com.github.azure.hadoop.custom.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

public class DFSCustomMSIApp {

    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();
        conf.set("fs.azure.account.auth.type", "Custom");
        conf.set("fs.azure.account.oauth.provider.type", "com.github.azure.hadoop.custom.auth.MSIBasedAccessTokenProvider");
        conf.set("fs.azure.account.oauth2.msi.tenant", args.length>1?args[1]:"");
        conf.set("fs.azure.account.oauth2.client.id",  args.length>2?args[1]:"");
        conf.set("fs.azure.account.oauth2.msi.endpoint", args.length>3?args[3]:"");

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
