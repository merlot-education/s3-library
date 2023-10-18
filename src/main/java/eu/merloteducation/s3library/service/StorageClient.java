package eu.merloteducation.s3library.service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import lombok.Getter;
import lombok.Setter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

public class StorageClient {
    private final AmazonS3 amazonS3Client;

    @Getter
    @Setter
    private String bucket;

    public StorageClient(String accessKey, String secret, String serviceEndpoint, String signingRegion,
        String signerType) {

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secret);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride(signerType);

        amazonS3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, signingRegion))
            .withClientConfiguration(clientConfiguration).build();
    }

    public byte[] getItem(String referenceId, String key) throws IOException {

        String composedKey = referenceId + "/" + key;
        S3Object object = amazonS3Client.getObject(new GetObjectRequest(getBucket(), composedKey));
        return object.getObjectContent().readAllBytes();
    }

    public String pushItem(String referenceId, String key, byte[] item) {

        String composedKey = referenceId + "/" + key;
        amazonS3Client.putObject(getBucket(), composedKey, new ByteArrayInputStream(item), null);
        return key;
    }

    public boolean deleteItem(String referenceId, String key) {

        String composedKey = referenceId + "/" + key;
        if (amazonS3Client.doesObjectExist(getBucket(), composedKey)) {
            amazonS3Client.deleteObject(getBucket(), composedKey);
            return true;
        } else {
            return false;
        }
    }

    public List<String> listItems(String referenceId) {

        return amazonS3Client.listObjectsV2(getBucket(), referenceId).getObjectSummaries().stream()
            .map(S3ObjectSummary::getKey).toList();
    }
}