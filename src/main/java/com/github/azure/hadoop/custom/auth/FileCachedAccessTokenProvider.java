package com.github.azure.hadoop.custom.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Date;
import java.util.UUID;

public abstract class FileCachedAccessTokenProvider implements CustomTokenProviderAdaptee {

    private static final Logger LOG = LoggerFactory.getLogger(FileCachedAccessTokenProvider.class);
    private static final String TOKEN_FILE_FOLDER = "/.azure/MSITokenCache/";
    public static final int HALF_HOUR = 30 * 60 * 1000;

    // UUID for the token file
    private final String tokenFileUUID = UUID.randomUUID().toString();

    private long tokenFetchTime;

    private boolean fromCache = false;

    protected abstract CustomTokenProviderAdaptee getImpl();

    @Override
    public String getAccessToken() throws IOException {
        LOG.debug("Getting access token for Azure Storage account.");
        //try to get the token from cache first
        String token = getAccessTokenFromCache();
        if(token==null) {
            fromCache = false;
            token = getImpl().getAccessToken();
            try{
                writeTokenToCache(token);
            } catch (IOException e) {
                LOG.error("Failed to write token to file", e);
            }
        } else {
            fromCache = true;
            LOG.info("Getting access token from local cache");
        }
        return token;
    }

    private synchronized void writeTokenToCache(String token) throws IOException{
        LOG.debug("Writing token to cache");
        File tokenFile = new File(System.getProperty("user.home") + TOKEN_FILE_FOLDER + tokenFileUUID);
        RandomAccessFile raf = null;
        try {
            if (!tokenFile.exists()) {
                tokenFile.getParentFile().mkdirs();
                tokenFile.createNewFile();
            }
            raf = new RandomAccessFile(tokenFile, "rw");
            raf.setLength(0);
            raf.write(token.getBytes());
        } catch (IOException e) {
            LOG.error("Failed to write token to file", e);
            throw e;
        } finally {
            raf.close();
            tokenFile.deleteOnExit();
        }


    }

    private String getAccessTokenFromCache() throws IOException {
        LOG.debug("Getting access token from cache");
        // create token cache folder if not exists
        File tokenCacheFolder = new File(System.getProperty("user.home") + TOKEN_FILE_FOLDER);
        if (!tokenCacheFolder.exists()) {
            tokenCacheFolder.mkdirs();
        }
        // get files in the token cache folder
        File[] files = tokenCacheFolder.listFiles();
        //check if file create in 30 mins
        if (files != null) {
            for (File file : files) {
                if (file.lastModified() > System.currentTimeMillis() - HALF_HOUR) {
                    // read token from file
                    this.tokenFetchTime = System.currentTimeMillis();
                    return readTokenFromFile(file);
                }
            }
        }
        return null;

    }

    private String readTokenFromFile(File file) throws IOException{
        LOG.debug("Reading token from file");
        //Read token from file
        String token = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            token = randomAccessFile.readLine();
        } catch (IOException e) {
            LOG.error("Failed to read token from file", e);
        } finally {
            if(randomAccessFile!=null) {
                randomAccessFile.close();
            }
        }
        return token;
    }

    @Override
    public Date getExpiryTime() {
        if(fromCache) {
            return new Date(this.tokenFetchTime + HALF_HOUR);
        } else {
            return getImpl().getExpiryTime();
        }
    }

    @Override
    public void initialize(Configuration configuration, String accountName) throws IOException {
        this.getImpl().initialize(configuration, accountName);
    }
}
