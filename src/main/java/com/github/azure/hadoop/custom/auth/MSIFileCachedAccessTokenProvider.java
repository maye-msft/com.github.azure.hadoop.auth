package com.github.azure.hadoop.custom.auth;

import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

public class MSIFileCachedAccessTokenProvider extends FileCachedAccessTokenProvider {
    private MSIBasedAccessTokenProvider tokenProvider;
    public MSIFileCachedAccessTokenProvider() {
        tokenProvider = new MSIBasedAccessTokenProvider();
    }

    @Override
    protected CustomTokenProviderAdaptee getImpl() {
        return tokenProvider;
    }

}
