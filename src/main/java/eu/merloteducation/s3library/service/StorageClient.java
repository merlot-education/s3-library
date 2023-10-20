package eu.merloteducation.s3library.service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

@Service
public class StorageClient {
    private final AmazonS3 s3Client;

    private final String bucket;

    private final String rootDirectory;

    /**
     * Create a StorageClient object with given credentials (accessKey, secret), endpoint configuration
     * (serviceEndpoint, signingRegion) and client configuration (signerType). Set bucket and root directory to use.
     *
     * @param accessKey access key
     * @param secret secret
     * @param serviceEndpoint service endpoint
     * @param signingRegion signing region
     * @param signerType signer type
     * @param bucket bucket
     * @param rootDirectory root directory
     */
    public StorageClient(@Value("${s3-library.access-key}") String accessKey,
        @Value("${s3-library.secret}") String secret, @Value("${s3-library.service-endpoint}") String serviceEndpoint,
        @Value("${s3-library.signing-region}") String signingRegion,
        @Value("${s3-library.signer-type}") String signerType, @Value("${s3-library.bucket}") String bucket,
        @Value("${s3-library.root-directory}") String rootDirectory) {

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secret);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride(signerType);

        s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, signingRegion))
            .withClientConfiguration(clientConfiguration).build();

        this.bucket = bucket;
        this.rootDirectory = rootDirectory;

    }

    /**
     * List items within the scope of the provided referenceId.
     *
     * @param referenceId scope of the items
     * @return list of items
     */
    public List<String> listItems(String referenceId) {

        String composedKey = getComposedKey(referenceId, "");
        return s3Client.listObjectsV2(this.bucket, composedKey).getObjectSummaries().stream()
            .map(obj -> removePrefix(obj.getKey(), composedKey)).toList();
    }

    /**
     * Push an item with given file name to the provided scope.
     *
     * @param referenceId scope to push the item to
     * @param fileName name of the item
     * @param item item to push
     */
    public void pushItem(String referenceId, String fileName, byte[] item) {

        String composedKey = getComposedKey(referenceId, fileName);
        s3Client.putObject(this.bucket, composedKey, new ByteArrayInputStream(item), null);
    }

    /**
     * Get an item with given key from the provided scope.
     *
     * @param referenceId scope from where to get the item
     * @param key key of the item
     * @return item
     * @throws IOException if an I/O error occurs
     */
    public byte[] getItem(String referenceId, String key) throws IOException {

        String composedKey = getComposedKey(referenceId, key);
        S3Object object = s3Client.getObject(new GetObjectRequest(this.bucket, composedKey));
        return object.getObjectContent().readAllBytes();
    }

    /**
     * Delete an item with given key from the provided scope.
     *
     * @param referenceId scope from where to delete the item
     * @param key key of the item
     * @return true if deletion was successful, false if item was not found
     */
    public boolean deleteItem(String referenceId, String key) {

        String composedKey = getComposedKey(referenceId, key);
        if (s3Client.doesObjectExist(this.bucket, composedKey)) {
            s3Client.deleteObject(this.bucket, composedKey);
            return true;
        } else {
            return false;
        }
    }

    private String getComposedKey(String referenceId, String key) {

        StringBuilder str = new StringBuilder();
        str.append(this.rootDirectory);
        str.append("/");
        str.append(referenceId);
        str.append("/");
        str.append(key);
        return str.toString();
    }

    private String removePrefix(String s, String prefix) {

        if (s != null && prefix != null && s.startsWith(prefix)) {
            return s.substring(prefix.length());
        }
        return s;
    }
}
