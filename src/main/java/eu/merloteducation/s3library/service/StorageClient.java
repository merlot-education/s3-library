package eu.merloteducation.s3library.service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

@Service
public class StorageClient {
    private final AmazonS3 amazonS3Client;

    private final String bucket;

    public StorageClient(@Value("${s3-library.access-key}") String accessKey,
        @Value("${s3-library.secret}") String secret, @Value("${s3-library.service-endpoint}") String serviceEndpoint,
        @Value("${s3-library.signing-region}") String signingRegion,
        @Value("${s3-library.signer-type}") String signerType, @Value("${s3-library.bucket}") String bucket) {

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secret);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride(signerType);

        amazonS3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, signingRegion))
            .withClientConfiguration(clientConfiguration).build();

        this.bucket = bucket;
    }

    public List<String> listItems(String referenceId) {

        return amazonS3Client.listObjectsV2(this.bucket, referenceId).getObjectSummaries().stream()
            .map(S3ObjectSummary::getKey).toList();
    }

    public String pushItem(String referenceId, String key, byte[] item) {

        String composedKey = referenceId + "/" + key;
        amazonS3Client.putObject(this.bucket, composedKey, new ByteArrayInputStream(item), null);
        return key;
    }

    public byte[] getItem(String referenceId, String key) throws IOException {

        String composedKey = referenceId + "/" + key;
        S3Object object = amazonS3Client.getObject(new GetObjectRequest(this.bucket, composedKey));
        return object.getObjectContent().readAllBytes();
    }

    public boolean deleteItem(String referenceId, String key) {

        String composedKey = referenceId + "/" + key;
        if (amazonS3Client.doesObjectExist(this.bucket, composedKey)) {
            amazonS3Client.deleteObject(this.bucket, composedKey);
            return true;
        } else {
            return false;
        }
    }
}
