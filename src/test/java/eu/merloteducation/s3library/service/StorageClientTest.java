package eu.merloteducation.s3library.service;

import com.amazonaws.services.s3.model.AmazonS3Exception;
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
    public void testPushItem() throws IOException {

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
    public void testListItems() throws IOException {

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
    public void testGetItem() throws IOException {

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
    public void testDeleteItem() throws IOException {

        String referenceId = "test:01";
        String key1 = "test";
        String key2 = "test-copy";

        pushTestData(referenceId, key1, key2);

        List<String> listOfItemsBeforeDelete = storageClient.listItems(referenceId);
        assertTrue(listOfItemsBeforeDelete.contains(key1));
        assertTrue(listOfItemsBeforeDelete.contains(key2));

        assertTrue(storageClient.deleteItem(referenceId, key1));
        assertTrue(storageClient.deleteItem(referenceId, key2));

        List<String> listOfItemsAfterDelete = storageClient.listItems(referenceId);
        assertFalse(listOfItemsAfterDelete.contains(key1));
        assertFalse(listOfItemsAfterDelete.contains(key2));
    }

    @Test
    public void testDeleteNonExistentItem() {

        assertFalse(storageClient.deleteItem("dummy:00", "dummy"));
    }

    @Test
    public void testListItemsForNonExistentReferenceId() {

        assertTrue(storageClient.listItems("dummy:00").isEmpty());
    }

    @Test
    public void testGetNonExistentItem() throws IOException {

        assertThrows(AmazonS3Exception.class, () -> storageClient.getItem("dummy:00", "dummy"));
    }

    private byte[] getTestData() throws IOException {

        return "This is test data.".getBytes();
    }

    private void pushTestData(String referenceId, String key1, String key2) throws IOException {

        byte[] testData = getTestData();
        storageClient.pushItem(referenceId, key1, testData);
        storageClient.pushItem(referenceId, key2, testData);
    }

    private void deleteTestData(String referenceId, String key1, String key2) {

        storageClient.deleteItem(referenceId, key1);
        storageClient.deleteItem(referenceId, key2);
    }
}
