package com.github.azure.hadoop.custom.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.AuthConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.*;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_BACKOFF_INTERVAL;

public class MSIBasedAccessTokenProvider implements CustomTokenProviderAdaptee {

    private static final Logger LOG = LoggerFactory.getLogger(MSIBasedAccessTokenProvider.class);


    private String authEndpoint;

    private String authority;

    private String tenantGuid;

    private String clientId;

    private long tokenFetchTime;

    private static final long ONE_HOUR = 3600 * 1000;

    /**
     *  The minimum random ratio used for delay interval calculation.
     */
    private static final double MIN_RANDOM_RATIO = 0.8;

    /**
     *  The maximum random ratio used for delay interval calculation.
     */
    private static final double MAX_RANDOM_RATIO = 1.2;


    /**
     *  Holds the random number generator used to calculate randomized backoff intervals
     */
    private final Random randRef = new Random();

    private int minBackoffInterval;

    private int maxBackoffInterval;

    private int backoffInterval;


    private int customTokenFetchRetryCount;

    private  int retryCount = 0;

    @Override
    public void initialize(Configuration configuration, String accountName) throws IOException  {
        this.authEndpoint = getConfigurationValue(configuration,
                ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT,
                AuthConfigurations.DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT);
        this.tenantGuid =
                getConfigurationValue(configuration, ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT , "");
        this.clientId =
                getConfigurationValue(configuration, FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID, "");
        String authority = getConfigurationValue(configuration,
                FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY,
                AuthConfigurations.DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY);
        this.authority = appendSlashIfNeeded(authority);

        this.minBackoffInterval = configuration.getInt(AZURE_MIN_BACKOFF_INTERVAL, DEFAULT_MIN_BACKOFF_INTERVAL);
        this.maxBackoffInterval = configuration.getInt(AZURE_MAX_BACKOFF_INTERVAL, DEFAULT_MAX_BACKOFF_INTERVAL);
        this.backoffInterval = configuration.getInt(AZURE_BACKOFF_INTERVAL, DEFAULT_BACKOFF_INTERVAL);
        this.customTokenFetchRetryCount = configuration.getInt(AZURE_CUSTOM_TOKEN_FETCH_RETRY_COUNT, DEFAULT_CUSTOM_TOKEN_FETCH_RETRY_COUNT);


    }

    private String appendSlashIfNeeded(String authority) {
        if (!authority.endsWith(AbfsHttpConstants.FORWARD_SLASH)) {
            authority = authority + AbfsHttpConstants.FORWARD_SLASH;
        }
        return authority;
    }


    private String getConfigurationValue(Configuration configuration, String key, String defaultValue) {
        String value = configuration.get(key, defaultValue);
        return value.trim();
    }



    @Override
    public String getAccessToken() throws IOException {
        LOG.info("MSIBasedAccessTokenProvider: get token");
        synchronized (this) {
            try {
                AzureADToken token = AzureADAuthenticator
                        .getTokenFromMsi(authEndpoint, tenantGuid, clientId, authority, false);
                this.tokenFetchTime = System.currentTimeMillis();
                return token.getAccessToken();
            } catch (AzureADAuthenticator.HttpException e) {
                if (e.getHttpErrorCode() == 429 && retryCount < customTokenFetchRetryCount) { //Too many requests
                    LOG.error("MSIBasedAccessTokenProvider: Too many requests to MSI. Wait for retry");
                    try {
                        Thread.sleep(getWaitInterval(++retryCount));
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    if (retryCount == customTokenFetchRetryCount) {
                        retryCount = 0;
                    }
                }
                throw new IOException(e);
            }
        }
    }

    @Override
    public Date getExpiryTime() {
        return new Date(tokenFetchTime + ONE_HOUR);
    }


    public long getWaitInterval(final int retryCount) {
        final long boundedRandDelta = (int) (this.backoffInterval * MIN_RANDOM_RATIO)
                + this.randRef.nextInt((int) (this.backoffInterval * MAX_RANDOM_RATIO)
                - (int) (this.backoffInterval * MIN_RANDOM_RATIO));

        final double incrementDelta = (Math.pow(2, retryCount - 1)) * boundedRandDelta;

        final long retryInterval = (int) Math.round(Math.min(this.minBackoffInterval + incrementDelta, maxBackoffInterval));

        return retryInterval;
    }
}
