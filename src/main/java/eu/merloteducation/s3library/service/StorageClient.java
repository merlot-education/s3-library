package eu.merloteducation.s3library.service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
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
     * @throws StorageClientCreationException if an error occurs while creating the storage client
     */
    public StorageClient(@Value("${s3-library.access-key}") String accessKey,
        @Value("${s3-library.secret}") String secret, @Value("${s3-library.service-endpoint}") String serviceEndpoint,
        @Value("${s3-library.signing-region}") String signingRegion,
        @Value("${s3-library.signer-type}") String signerType, @Value("${s3-library.bucket}") String bucket,
        @Value("${s3-library.root-directory}") String rootDirectory) throws StorageClientCreationException {

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secret);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride(signerType);

        try {
            s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, signingRegion))
                .withClientConfiguration(clientConfiguration).build();
        } catch (IllegalArgumentException | SdkClientException exception) {
            throw new StorageClientCreationException(exception.getMessage());
        }

        this.bucket = bucket;
        this.rootDirectory = rootDirectory;
    }

    /**
     * List items within the scope of the provided referenceId.
     *
     * @param referenceId scope of the items
     * @return list of items
     * @throws StorageClientException if an error occurs while getting the list
     */
    public List<String> listItems(String referenceId) throws StorageClientException {

        String composedKey = getComposedKey(referenceId, "");
        ListObjectsV2Result listObjectsV2Result;
        try {
            listObjectsV2Result = s3Client.listObjectsV2(this.bucket, composedKey);
        } catch (SdkClientException exception) {
            throw new StorageClientException(exception.getMessage());
        }
        return listObjectsV2Result.getObjectSummaries().stream().map(obj -> removePrefix(obj.getKey(), composedKey))
            .toList();
    }

    /**
     * Push an item with given file name to the provided scope.
     *
     * @param referenceId scope to push the item to
     * @param fileName name of the item
     * @param item item to push
     * @throws StorageClientException if an error occurs while pushing the item
     */
    public void pushItem(String referenceId, String fileName, byte[] item) throws StorageClientException {

        String composedKey = getComposedKey(referenceId, fileName);
        try {
            s3Client.putObject(this.bucket, composedKey, new ByteArrayInputStream(item), null);
        } catch (SdkClientException exception) {
            throw new StorageClientException(exception.getMessage());
        }
    }

    /**
     * Get an item with given key from the provided scope.
     *
     * @param referenceId scope from where to get the item
     * @param key key of the item
     * @return item
     * @throws IOException if an I/O error occurs
     * @throws StorageClientException if an error occurs while getting the item
     */
    public byte[] getItem(String referenceId, String key) throws IOException, StorageClientException {

        String composedKey = getComposedKey(referenceId, key);
        S3Object object;
        try {
            object = s3Client.getObject(new GetObjectRequest(this.bucket, composedKey));
        } catch (SdkClientException exception) {
            throw new StorageClientException(exception.getMessage());
        }
        return object.getObjectContent().readAllBytes();
    }

    /**
     * Delete an item with given key from the provided scope.
     *
     * @param referenceId scope from where to delete the item
     * @param key key of the item
     * @throws StorageClientException if an error occurs while deleting the item
     */
    public void deleteItem(String referenceId, String key) throws StorageClientException {

        String composedKey = getComposedKey(referenceId, key);
        try {
            if (s3Client.doesObjectExist(this.bucket, composedKey)) {
                s3Client.deleteObject(this.bucket, composedKey);
            } else {
                throw new StorageClientException(
                    String.format("The item you want to delete (%s) does not exist.", key));
            }
        } catch (SdkClientException exception) {
            throw new StorageClientException(exception.getMessage());
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
