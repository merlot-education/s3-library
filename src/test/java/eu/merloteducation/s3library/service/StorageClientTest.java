package eu.merloteducation.s3library.service;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = { StorageClient.class })
public class StorageClientTest {
    @Autowired
    StorageClient storageClient;

    @Value("${s3-library.access-key}")
    String accessKey;

    @Value("${s3-library.secret}")
    String secret;

    @Value("${s3-library.service-endpoint}")
    String serviceEndpoint;

    @Value("${s3-library.signing-region}")
    String signingRegion;

    @Value("${s3-library.signer-type}")
    String signerType;

    @Value("${s3-library.bucket}")
    String bucket;

    @Value("${s3-library.root-directory}")
    String rootDirectory;

    @Test
    public void testPushItem() throws StorageClientException {

        String referenceId = "test:01";
        String key1 = "test";
        String key2 = "test-copy";

        List<String> listOfItemsBeforePush = storageClient.listItems(referenceId);
        assertFalse(listOfItemsBeforePush.contains(key1));
        assertFalse(listOfItemsBeforePush.contains(key2));

        storageClient.pushItem(referenceId, key1, getTestData());
        storageClient.pushItem(referenceId, key2, getTestData());

        List<String> listOfItemsAfterPush = storageClient.listItems(referenceId);
        assertTrue(listOfItemsAfterPush.contains(key1));
        assertTrue(listOfItemsAfterPush.contains(key2));

        deleteTestData(referenceId, key1, key2);
    }

    @Test
    public void testListItems() throws StorageClientException {

        String referenceId = "test:01";
        String key1 = "test";
        String key2 = "test-copy";

        pushTestData(referenceId, key1, key2);

        List<String> listOfItems = storageClient.listItems(referenceId);
        assertTrue(listOfItems.contains(key1));
        assertTrue(listOfItems.contains(key2));

        deleteTestData(referenceId, key1, key2);
    }

    @Test
    public void testGetItem() throws IOException, StorageClientException {

        String referenceId = "test:01";
        String key1 = "test";
        String key2 = "test-copy";

        pushTestData(referenceId, key1, key2);

        byte[] dataFromLocal = getTestData();
        byte[] dataFromStorage1 = storageClient.getItem(referenceId, key1);
        byte[] dataFromStorage2 = storageClient.getItem(referenceId, key2);

        assertArrayEquals(dataFromStorage1, dataFromLocal);
        assertArrayEquals(dataFromStorage2, dataFromLocal);

        deleteTestData(referenceId, key1, key2);
    }

    @Test
    public void testDeleteItem() throws StorageClientException {

        String referenceId = "test:01";
        String key1 = "test";
        String key2 = "test-copy";

        pushTestData(referenceId, key1, key2);

        List<String> listOfItemsBeforeDelete = storageClient.listItems(referenceId);
        assertTrue(listOfItemsBeforeDelete.contains(key1));
        assertTrue(listOfItemsBeforeDelete.contains(key2));

        storageClient.deleteItem(referenceId, key1);
        storageClient.deleteItem(referenceId, key2);

        List<String> listOfItemsAfterDelete = storageClient.listItems(referenceId);
        assertFalse(listOfItemsAfterDelete.contains(key1));
        assertFalse(listOfItemsAfterDelete.contains(key2));
    }

    @Test
    public void testDeleteNonExistentItem() {

        Exception exception = assertThrows(StorageClientException.class,
            () -> storageClient.deleteItem("dummy:00", "dummy"));
        String expectedMessage = String.format("The item you want to delete (%s) does not exist.", "dummy");
        String actualMessage = exception.getMessage();
        assertEquals(actualMessage, expectedMessage);
    }

    @Test
    public void testListItemsForNonExistentReferenceId() throws StorageClientException {

        assertTrue(storageClient.listItems("dummy:00").isEmpty());
    }

    @Test
    public void testGetNonExistentItem() {

        Exception exception = assertThrows(StorageClientException.class,
            () -> storageClient.getItem("dummy:00", "dummy"));
        String expectedMessage = "The specified key does not exist";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    //Invalid signing region does not affect the operations.
    //Root directory will just take any value to use as root directory name, there cannot be an invalid name.

    @Test
    public void testInvalidSignerType() {

        String invalidSignerType = "dummy";
        assertThrows(StorageClientCreationException.class,
            () -> new StorageClient(accessKey, secret, serviceEndpoint, signingRegion, invalidSignerType, bucket,
                rootDirectory));
    }

    @Test
    public void testInvalidAccessKey() throws StorageClientCreationException {

        String invalidAccessKey = "dummy";
        StorageClient client = new StorageClient(invalidAccessKey, secret, serviceEndpoint, signingRegion, signerType,
            bucket, rootDirectory);

        String expectedMessage = "Server Error";

        Exception exceptionDelete = assertThrows(StorageClientException.class,
            () -> client.deleteItem("dummy:00", "dummy"));
        String actualMessageDelete = exceptionDelete.getMessage();
        assertTrue(actualMessageDelete.contains(expectedMessage));

        Exception exceptionGet = assertThrows(StorageClientException.class, () -> client.getItem("dummy:00", "dummy"));
        String actualMessageGet = exceptionGet.getMessage();
        assertTrue(actualMessageGet.contains(expectedMessage));

        Exception exceptionList = assertThrows(StorageClientException.class, () -> client.listItems("dummy:00"));
        String actualMessageList = exceptionList.getMessage();
        assertTrue(actualMessageList.contains(expectedMessage));

        Exception exceptionPush = assertThrows(StorageClientException.class,
            () -> client.pushItem("dummy:00", "dummy", getTestData()));
        String actualMessagePush = exceptionPush.getMessage();
        assertTrue(actualMessagePush.contains(expectedMessage));
    }

    @Test
    public void testInvalidSecret() throws StorageClientCreationException {

        String invalidSecret = "dummy";
        StorageClient client = new StorageClient(accessKey, invalidSecret, serviceEndpoint, signingRegion, signerType,
            bucket, rootDirectory);

        Exception exceptionDelete = assertThrows(StorageClientException.class,
            () -> client.deleteItem("dummy:00", "dummy"));
        String expectedMessageDelete = "Forbidden";
        String actualMessageDelete = exceptionDelete.getMessage();
        assertTrue(actualMessageDelete.contains(expectedMessageDelete));

        Exception exceptionGet = assertThrows(StorageClientException.class, () -> client.getItem("dummy:00", "dummy"));
        String expectedMessageGet = "Check your AWS Secret Access Key and signing method";
        String actualMessageGet = exceptionGet.getMessage();
        assertTrue(actualMessageGet.contains(expectedMessageGet));

        Exception exceptionList = assertThrows(StorageClientException.class, () -> client.listItems("dummy:00"));
        String expectedMessageList = "Check your AWS Secret Access Key and signing method";
        String actualMessageList = exceptionList.getMessage();
        assertTrue(actualMessageList.contains(expectedMessageList));

        Exception exceptionPush = assertThrows(StorageClientException.class,
            () -> client.pushItem("dummy:00", "dummy", getTestData()));
        String expectedMessagePush = "Bad request";
        String actualMessagePush = exceptionPush.getMessage();
        assertTrue(actualMessagePush.contains(expectedMessagePush));
    }

    @Test
    public void testInvalidServiceEndpoint() throws StorageClientCreationException {

        String invalidServiceEndpoint = "dummy.dummy";
        StorageClient client = new StorageClient(accessKey, secret, invalidServiceEndpoint, signingRegion, signerType,
            bucket, rootDirectory);

        String expectedMessage = "Unable to execute HTTP request";

        Exception exceptionDelete = assertThrows(StorageClientException.class,
            () -> client.deleteItem("dummy:00", "dummy"));
        String actualMessageDelete = exceptionDelete.getMessage();
        assertTrue(actualMessageDelete.contains(expectedMessage));

        Exception exceptionGet = assertThrows(StorageClientException.class, () -> client.getItem("dummy:00", "dummy"));
        String actualMessageGet = exceptionGet.getMessage();
        assertTrue(actualMessageGet.contains(expectedMessage));

        Exception exceptionList = assertThrows(StorageClientException.class, () -> client.listItems("dummy:00"));
        String actualMessageList = exceptionList.getMessage();
        assertTrue(actualMessageList.contains(expectedMessage));

        Exception exceptionPush = assertThrows(StorageClientException.class,
            () -> client.pushItem("dummy:00", "dummy", getTestData()));
        String actualMessagePush = exceptionPush.getMessage();
        assertTrue(actualMessagePush.contains(expectedMessage));
    }

    @Test
    public void testInvalidBucket() throws StorageClientCreationException {

        String invalidBucket = "dummy";
        StorageClient client = new StorageClient(accessKey, secret, serviceEndpoint, signingRegion, signerType,
            invalidBucket, rootDirectory);

        String expectedMessage = "Access Denied";

        Exception exceptionDelete = assertThrows(StorageClientException.class,
            () -> client.deleteItem("dummy:00", "dummy"));
        String expectedMessageDelete = "Forbidden";
        String actualMessageDelete = exceptionDelete.getMessage();
        assertTrue(actualMessageDelete.contains(expectedMessageDelete));

        Exception exceptionGet = assertThrows(StorageClientException.class, () -> client.getItem("dummy:00", "dummy"));
        String actualMessageGet = exceptionGet.getMessage();
        assertTrue(actualMessageGet.contains(expectedMessage));

        Exception exceptionList = assertThrows(StorageClientException.class, () -> client.listItems("dummy:00"));
        String actualMessageList = exceptionList.getMessage();
        assertTrue(actualMessageList.contains(expectedMessage));

        Exception exceptionPush = assertThrows(StorageClientException.class,
            () -> client.pushItem("dummy:00", "dummy", getTestData()));
        String actualMessagePush = exceptionPush.getMessage();
        assertTrue(actualMessagePush.contains(expectedMessage));
    }

    private byte[] getTestData() {

        return "This is test data.".getBytes();
    }

    private void pushTestData(String referenceId, String key1, String key2) throws StorageClientException {

        byte[] testData = getTestData();
        storageClient.pushItem(referenceId, key1, testData);
        storageClient.pushItem(referenceId, key2, testData);
    }

    private void deleteTestData(String referenceId, String key1, String key2) throws StorageClientException {

        storageClient.deleteItem(referenceId, key1);
        storageClient.deleteItem(referenceId, key2);
    }
}
