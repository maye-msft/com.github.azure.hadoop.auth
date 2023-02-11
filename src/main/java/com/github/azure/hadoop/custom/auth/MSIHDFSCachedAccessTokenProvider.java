package com.github.azure.hadoop.custom.auth;

import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MSIHDFSCachedAccessTokenProvider extends HDFSCachedAccessTokenProvider {

    Logger LOG = LoggerFactory.getLogger(MSIHDFSCachedAccessTokenProvider.class);

    private MSIBasedAccessTokenProvider tokenProvider;
    public MSIHDFSCachedAccessTokenProvider() {
        LOG.info("Init MSIHDFSCachedAccessTokenProvider "+ "Version: " + Version.VERSION );
        tokenProvider = new MSIBasedAccessTokenProvider();
    }

    @Override
    protected CustomTokenProviderAdaptee getImpl() {
        return tokenProvider;
    }

}
