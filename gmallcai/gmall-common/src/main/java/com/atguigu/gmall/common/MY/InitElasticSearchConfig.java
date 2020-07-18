package com.caicai.gmall.common.MY;

import io.searchbox.client.JestClient;

public class InitElasticSearchConfig<jestClient> {
    private jestClient jestClient;
    public JestClient getJestClient() {
        return (JestClient) jestClient;
    }
}
