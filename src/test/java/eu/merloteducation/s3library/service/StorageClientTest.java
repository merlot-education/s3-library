package eu.merloteducation.s3library.service;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = { StorageClient.class})
public class StorageClientTest {
    @Autowired
    StorageClient storageClient;

    @Test
    @Order(1)
    public void testPushItem() throws IOException {

        String referenceId = "test:01";
        String key1 = "test.png";
        String key2 = "test-copy.png";

        String composedKey1 = referenceId + "/" + key1;
        String composedKey2 = referenceId + "/" + key2;

        List<String> listOfItemsBeforePush = storageClient.listItems(referenceId);
        assertFalse(listOfItemsBeforePush.contains(composedKey1));
        assertFalse(listOfItemsBeforePush.contains(composedKey2));

        assertEquals(storageClient.pushItem(referenceId, key1, getTestImage()), key1);
        assertEquals(storageClient.pushItem(referenceId, key2, getTestImage()), key2);

        List<String> listOfItemsAfterPush = storageClient.listItems(referenceId);
        assertTrue(listOfItemsAfterPush.contains(composedKey1));
        assertTrue(listOfItemsAfterPush.contains(composedKey2));
    }

    @Test
    @Order(2)
    public void testListItems() {

        String referenceId = "test:01";
        String key1 = "test.png";
        String key2 = "test-copy.png";

        String composedKey1 = referenceId + "/" + key1;
        String composedKey2 = referenceId + "/" + key2;

        List<String> listOfItems = storageClient.listItems(referenceId);
        assertTrue(listOfItems.contains(composedKey1));
        assertTrue(listOfItems.contains(composedKey2));
    }

    @Test
    @Order(3)
    public void testGetItem() throws IOException {

        String referenceId = "test:01";
        String key1 = "test.png";
        String key2 = "test-copy.png";

        byte[] imageFromLocal = getTestImage();
        byte[] imageFromStorage1 = storageClient.getItem(referenceId, key1);
        byte[] imageFromStorage2 = storageClient.getItem(referenceId, key2);

        assertArrayEquals(imageFromStorage1, imageFromLocal);
        assertArrayEquals(imageFromStorage2, imageFromLocal);
    }

    @Test
    @Order(4)
    public void testDeleteItem() throws IOException {

        String referenceId = "test:01";
        String key1 = "test.png";
        String key2 = "test-copy.png";

        String composedKey1 = referenceId + "/" + key1;
        String composedKey2 = referenceId + "/" + key2;

        List<String> listOfItemsBeforeDelete = storageClient.listItems(referenceId);
        assertTrue(listOfItemsBeforeDelete.contains(composedKey1));
        assertTrue(listOfItemsBeforeDelete.contains(composedKey2));

        assertTrue(storageClient.deleteItem(referenceId, key1));
        assertTrue(storageClient.deleteItem(referenceId, key2));

        List<String> listOfItemsAfterDelete = storageClient.listItems(referenceId);
        assertFalse(listOfItemsAfterDelete.contains(composedKey1));
        assertFalse(listOfItemsAfterDelete.contains(composedKey2));
    }

    @Test
    public void testDeleteNonExistentItem() {

        assertFalse(storageClient.deleteItem("dummy:00", "dummy.png"));
    }

    @Test
    public void testListItemsForNonExistentReferenceId() {

        assertTrue(storageClient.listItems("dummy:00").isEmpty());
    }

    @Test
    public void testGetNonExistentItem() throws IOException {

        assertThrows(AmazonS3Exception.class, () -> storageClient.getItem("dummy:00", "dummy.png"));
    }

    private byte[] getTestImage() throws IOException {

        BufferedImage bImage = ImageIO.read(new File("./src/test/resources/test.png"));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ImageIO.write(bImage, "png", bos);
        return bos.toByteArray();
    }

}
