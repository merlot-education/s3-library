package eu.merloteducation.s3library.config;

import eu.merloteducation.s3library.service.StorageClient;
import eu.merloteducation.s3library.service.StorageClientFactory;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties
@ComponentScan(basePackageClasses = StorageClient.class)
@Getter
@Setter
public class StorageClientConfig {
    @Value("${s3-library.access-key}")
    private String accessKey;

    @Value("${s3-library.secret}")
    private String secret;

    @Value("${s3-library.service-endpoint}")
    private String serviceEndpoint;

    @Value("${s3-library.signing-region}")
    private String signingRegion;

    @Value("${s3-library.signer-type}")
    private String signerType;

    @Value("${s3-library.bucket}")
    private String bucket;

    @Bean(name = "storageClient")
    public StorageClientFactory storageClientFactory() {

        return new StorageClientFactory();
    }

    @Bean
    public StorageClient storageClient() throws Exception {

        return storageClientFactory().getObject();
    }
}