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

            if (this.token != null && !isTokenExpiring()) {
                LOG.info("return existing token in the instance. and expiry time " + getExpiryTime() + ". Version: " + Version.VERSION);
                LOG.debug("Token from instance: " + this.token);
                return this.token;
            } else { // set the token as null to force to get a new token
                LOG.debug("existing token is expiring or null. Version: " + Version.VERSION);
                this.token = null;
                this.expiryTime = 0;
            }

            //try to get the token from cache first
            try {
                loadAccessTokenFromCache(); // try to get token from local first
            } catch (Exception e) {
                LOG.error("Error occurred when try load token from local cache. The error can be ignored.", e);
                this.token = null;
                this.expiryTime = 0;
            }
            if (this.token != null && this.token.trim().length() == 0) {// if token is empty, log this error
                LOG.error("Failed to get access token from local cache. Invalid Token!, token length is zero! Try to request token from Azure AD.");
            }
            if (this.token != null && this.token.trim().length() > 0 && !isTokenExpiring()) {// valid token found in local cache and return it
                LOG.info("Getting access token from local cache successfully, expiry time: " + getExpiryTime()+ " Version: " + Version.VERSION);
                LOG.debug("Token from cache: " + this.token);
                return this.token;
            }

            LOG.info("Start to get token from Azure AD."+ " Version: " + Version.VERSION);
            // try to get token form remote

            try {
                this.token = getImpl().getAccessToken();
                this.expiryTime = getImpl().getExpiryTime().getTime();
            } catch (Exception e) {
                LOG.error("Failed to get token from Azure AD."+ " Version: " + Version.VERSION);
                LOG.debug("Failed to get token from Azure AD."+ " Version: " + Version.VERSION+". "+e.toString());

                if (e instanceof IOException) {
                    throw e;
                } else {
                    throw new IOException("Unknown exception occurred!", e);
                }
            }


            if (this.token == null || this.token.trim().length() == 0) {
                String msg = "Invalid Token!, token is null or zero length!";
                LOG.error(msg);
                throw new IOException(msg);
            }

            try {
                writeTokenToCache(this.token, this.expiryTime);
                LOG.info("Token is written to cache. UUID: " + tokenFileUUID);
            } catch (IOException e) {
                LOG.error("Failed to write token to file. UUID: " + tokenFileUUID, e);
            }

            LOG.info("Getting access token from Azure AD successfully. expiry time: " + getExpiryTime()+ " Version: " + Version.VERSION);
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
            out.write(this.token.getBytes());
            out.flush();
            LOG.info("Token has been written to cache. file path: " + tokenCacheFile);
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

                    if (!this.isTokenExpiring()) {
                        LOG.info("Found token file in cache: " + file.getPath().toString());
                        FSDataInputStream inputStream = null;
                        try {
                            inputStream = fs.open(file.getPath());
                            this.token = IOUtils.toString(inputStream);
                            return;
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
                    } else {
                        LOG.debug("Found expired token, "+fileName+" skip it.");
                    }

                }

                LOG.debug("Cannot found cached token in "+tokenCacheFolder);
            }
        }
    }

    @Override
    public Date getExpiryTime() {
        return new Date(this.expiryTime);
    }

    private boolean isTokenExpiring() {
        long approximatelyNow = System.currentTimeMillis() + TEN_MINUTES;
        return this.expiryTime <= approximatelyNow;
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
