package eu.merloteducation.s3library.service;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = { StorageClient.class })
public class StorageClientTest {
    @Autowired
    StorageClient storageClient;

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
        assertThrows(StorageClientException.class, () -> storageClient.deleteItem("dummy:00", "dummy"));
    }

    @Test
    public void testListItemsForNonExistentReferenceId() throws StorageClientException {

        assertTrue(storageClient.listItems("dummy:00").isEmpty());
    }

    @Test
    public void testGetNonExistentItem() {
        assertThrows(StorageClientException.class, () -> storageClient.getItem("dummy:00", "dummy"));
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
