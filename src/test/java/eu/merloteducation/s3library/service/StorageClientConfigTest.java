package eu.merloteducation.s3library.service;

import eu.merloteducation.s3library.config.StorageClientConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties = { "s3-library.access-key = DUMMY_ACCESS_KEY", "s3-library.secret = DUMMY_SECRET",
    "s3-library.service-endpoint= DUMMY_ENDPOINT" }, classes = { StorageClientConfig.class })
public class StorageClientConfigTest {
    @Autowired
    private StorageClientConfig storageClientConfig;

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    public void configurationLoads() {

        assertEquals(storageClientConfig.getAccessKey(), "DUMMY_ACCESS_KEY");
        assertEquals(storageClientConfig.getSecret(), "DUMMY_SECRET");
        assertEquals(storageClientConfig.getServiceEndpoint(), "DUMMY_ENDPOINT");
        assertEquals(storageClientConfig.getSigningRegion(), "de");
        assertEquals(storageClientConfig.getSignerType(), "S3SignerType");
        assertEquals(storageClientConfig.getBucket(), "merlot-storage-test");
    }
}
