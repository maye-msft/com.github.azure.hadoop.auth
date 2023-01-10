package com.github.azure.hadoop.custom.auth;

import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

public class MSIHDFSCachedAccessTokenProvider extends HDFSCachedAccessTokenProvider {
    private MSIBasedAccessTokenProvider tokenProvider;
    public MSIHDFSCachedAccessTokenProvider() {
        tokenProvider = new MSIBasedAccessTokenProvider();
    }

    @Override
    protected CustomTokenProviderAdaptee getImpl() {
        return tokenProvider;
    }

}
