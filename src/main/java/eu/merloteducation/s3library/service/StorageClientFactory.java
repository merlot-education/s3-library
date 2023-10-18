package eu.merloteducation.s3library.service;

import eu.merloteducation.s3library.config.StorageClientConfig;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;

public class StorageClientFactory implements FactoryBean<StorageClient> {
    @Autowired
    StorageClientConfig storageClientConfig;

    @Override
    public StorageClient getObject() throws Exception {

        StorageClient storageClient = new StorageClient(storageClientConfig.getAccessKey(),
            storageClientConfig.getSecret(), storageClientConfig.getServiceEndpoint(),
            storageClientConfig.getSigningRegion(), storageClientConfig.getSignerType());
        storageClient.setBucket(storageClientConfig.getBucket());
        return storageClient;

    }

    @Override
    public Class<?> getObjectType() {

        return StorageClient.class;
    }

    @Override
    public boolean isSingleton() {

        return false;
    }
}