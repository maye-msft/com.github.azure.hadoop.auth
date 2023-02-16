package com.github.azure.hadoop.custom.auth;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;


public abstract class HDFSCachedAccessTokenProvider implements CustomTokenProviderAdaptee {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSCachedAccessTokenProvider.class);
    private static final String TOKEN_FILE_FOLDER = "/.azure/MSITokenCache/";
    //public static final int HALF_HOUR = 30 * 60 * 1000;

    public static final int TEN_MINUTES = 10 * 60 * 1000;

    // UUID for the token file
    private final String tokenFileUUID = UUID.randomUUID().toString();

    private long expiryTime;

    private String token;

    private FileSystem fs;

    private String hdfsRootPath;
    private Configuration tokenHDFSConf;
    private boolean deleteOnExit;

    protected abstract CustomTokenProviderAdaptee getImpl();

    //    public static final String AZURE_CUSTOM_TOKEN_HDFS_HADOOP_CORE_SITE_PATH = "fs.azure.custom.token.hdfs.hadoop.core.site.path";
//    public static final String AZURE_CUSTOM_TOKEN_HDFS_HDFS_CORE_SITE_PATH = "fs.azure.custom.token.hdfs.hdfs.site.path";
//    public static final String AZURE_CUSTOM_TOKEN_HDFS_MAPRED_CORE_SITE_PATH = "fs.azure.custom.token.hdfs.mapred.site.path";
    public static final String AZURE_CUSTOM_TOKEN_HDFS_CACHE_PATH = "fs.azure.custom.token.hdfs.cache.path";

    @Override
    public String getAccessToken() throws IOException {
        LOG.info("Getting access token for Azure Storage account with retry and HDFS cache." + " Version: " + Version.VERSION);
        synchronized (this) {
            long approximatelyNow = System.currentTimeMillis() + TEN_MINUTES;
            if (this.token != null && this.expiryTime > approximatelyNow) {
                LOG.info("return existing token in the instance. and expiry time " + getExpiryTime() + ". Version: " + Version.VERSION);
                LOG.debug("Token from instance: " + this.token);
                return this.token;
            }

            //try to get the token from cache first
            loadAccessTokenFromCache(); // try to get token from local first
            if (this.token != null && this.token.trim().length() == 0) {// if token is empty, log this error
                LOG.error("Failed to get access token from local cache. Invalid Token!, token is null or zero length! Try to request token from Azure AD.");
            }
            if (this.token != null && this.token.trim().length() > 0) {// valid token found in local cache and return it
                LOG.info("Getting access token from local cache, and expiry time " + getExpiryTime()+ "Version: " + Version.VERSION);
                LOG.debug("Token from cache: " + this.token);
                return this.token;
            }

            // try to get token form remote
            this.token = getImpl().getAccessToken();
            this.expiryTime = getImpl().getExpiryTime().getTime();
            LOG.info("Getting access token from Azure AD successfully."+ " Version: " + Version.VERSION);

            try {
                writeTokenToCache(this.token, this.expiryTime);
                LOG.info("Token is written to cache. UUID: " + tokenFileUUID);
            } catch (IOException e) {
                LOG.error("Failed to write token to file. UUID: " + tokenFileUUID, e);
            }

            if (this.token == null || this.token.trim().length() == 0) {
                String msg = "Invalid Token!, token is null or zero length!";
                LOG.error(msg);
                throw new IOException(msg);
            }

            LOG.info("Getting access token from Azure AD successfully. "+"Token expiry time: " + getExpiryTime());
            LOG.debug("Token from Azure AD, Token is " + token);
            return this.token;
        }
    }

    private synchronized void writeTokenToCache(String tokenToWrite, long expiryTimeToWrite) throws IOException {
        String ts = getCacheFolderName();
        Path tokenCacheFolder = new Path(hdfsRootPath + "/" + TOKEN_FILE_FOLDER + ts + "/");
        Path tokenCacheFile = new Path(hdfsRootPath + "/" + TOKEN_FILE_FOLDER + ts + "/" + tokenFileUUID + "." + expiryTimeToWrite);

        FSDataOutputStream out = null;
        try {
            if (!fs.exists(tokenCacheFolder)) {
                fs.mkdirs(tokenCacheFolder);
            }

            out = fs.create(tokenCacheFile, true);
            out.write(token.getBytes());
            out.flush();
        } catch (IOException e) {
            LOG.error("Failed to write token to file", e);
            throw e;
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    LOG.error("Failed to close output stream", e);
                }
            }
            if (deleteOnExit) {
                fs.deleteOnExit(tokenCacheFile);
            }

        }


    }

    private synchronized void loadAccessTokenFromCache() throws IOException {
        LOG.info("Start getting access token from HDFS cache");
        // create token cache folder if not exists
        String ts = getCacheFolderName();
        Path tokenCacheFolder = new Path(hdfsRootPath + "/" + TOKEN_FILE_FOLDER + ts + "/");
        LOG.info("HDFS cache folder " + tokenCacheFolder.toString());
        if (fs.exists(tokenCacheFolder)) {
            // get files in the token cache folder
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(tokenCacheFolder, false);
            //check if file create in 30 mins
            if (files != null) {
                while (files.hasNext()) {
                    LocatedFileStatus file = files.next();
                    if(file == null){
                        continue;
                    }
                    String fileName = file.getPath().getName();
                    if (fileName.contains(".")) {
                        String[] segments = fileName.split("\\.");
                        if (segments.length == 2) {
                            try {
                                this.expiryTime = Long.parseLong(segments[1]);
                            } catch (NumberFormatException e) {
                                LOG.error("Failed to parse expiry time from file name", e);
                                continue;
                            }
                        }
                    } else {
                        continue;
                    }
                    long approximatelyNow = System.currentTimeMillis() + TEN_MINUTES;
                    if (this.expiryTime > approximatelyNow) {
                        LOG.info("Found token file in cache: " + file.getPath().toString());
                        FSDataInputStream inputStream = null;
                        try {
                            inputStream = fs.open(file.getPath());
                            this.token = IOUtils.toString(inputStream);
                        } catch (IOException e) {
                            LOG.error("Failed to read token from file", e);
                            //throw e; // do not throw exception here, as we want to try to get token from AD
                        } finally {
                            if (inputStream != null) {
                                try {
                                    inputStream.close();
                                } catch (IOException e) {
                                    LOG.error("Failed to close input stream", e);
                                }

                            }
                        }
                    }

                }
            }
        }
    }

    @Override
    public Date getExpiryTime() {
        return new Date(this.expiryTime);
    }

    @Override
    public void initialize(Configuration configuration, String accountName) throws IOException {
        this.deleteOnExit = configuration.getBoolean(FileCachedAccessTokenProvider.AZURE_CUSTOM_TOKEN_CACHE_DELETE_ON_EXIT, false);
        Configuration conf = new Configuration();
        this.tokenHDFSConf = conf;
        this.fs = FileSystem.get(conf);
        this.getImpl().initialize(configuration, accountName);
        this.hdfsRootPath = configuration.get(AZURE_CUSTOM_TOKEN_HDFS_CACHE_PATH);
        if (this.hdfsRootPath == null) {
            //throw new IOException("HDFS root path is not set. Please set "+AZURE_CUSTOM_TOKEN_HDFS_CACHE_PATH+" in core-site.xml");
            this.hdfsRootPath = "/tmp/" + accountName + "/";
            LOG.info("HDFS token cache folder is not set. Using default path: " + this.hdfsRootPath);
        }
    }

    private static String getCacheFolderName() {
        Format f = new SimpleDateFormat("yyyyMMddHH");
        String str = f.format(new Date());
        return str;
    }


}
