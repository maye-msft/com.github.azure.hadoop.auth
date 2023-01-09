package com.github.azure.hadoop.custom.auth;

import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

public class OAuthFileCachedAccessTokenProvider extends FileCachedAccessTokenProvider {
    private OAuthBasedAccessTokenProvider tokenProvider;
    public OAuthFileCachedAccessTokenProvider() {
        tokenProvider = new OAuthBasedAccessTokenProvider();
    }

    @Override
    protected CustomTokenProviderAdaptee getImpl() {
        return tokenProvider;
    }

}
