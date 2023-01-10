package com.github.azure.hadoop.custom.auth;

import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

public class OAuthHDFSCachedAccessTokenProvider extends HDFSCachedAccessTokenProvider {
    private OAuthBasedAccessTokenProvider tokenProvider;
    public OAuthHDFSCachedAccessTokenProvider() {
        tokenProvider = new OAuthBasedAccessTokenProvider();
    }

    @Override
    protected CustomTokenProviderAdaptee getImpl() {
        return tokenProvider;
    }

}
