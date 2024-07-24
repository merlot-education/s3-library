/*
 *  Copyright 2024 Dataport. All rights reserved. Developed as part of the MERLOT project.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package eu.merloteducation.s3library.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = { StorageClient.class })
class StorageClientTest {
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

    @BeforeEach
    public void setup() {
        ReflectionTestUtils.setField(storageClient, "s3Client", new AmazonS3Fake());
    }

    @Test
    void testPushItem() throws StorageClientException {

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
    void testListItems() throws StorageClientException {

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
    void testGetItem() throws IOException, StorageClientException {

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
    void testDeleteItem() throws StorageClientException {

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
    void testDeleteNonExistentItem() {

        Exception exception = assertThrows(StorageClientException.class,
            () -> storageClient.deleteItem("dummy:00", "dummy"));
        String expectedMessage = String.format("The item you want to delete (%s) does not exist.", "dummy");
        String actualMessage = exception.getMessage();
        assertEquals(actualMessage, expectedMessage);
    }

    @Test
    void testListItemsForNonExistentReferenceId() throws StorageClientException {

        assertTrue(storageClient.listItems("dummy:00").isEmpty());
    }

    @Test
    void testGetNonExistentItem() {

        Exception exception = assertThrows(StorageClientException.class,
            () -> storageClient.getItem("dummy:00", "dummy"));
        String expectedMessage = "The specified key does not exist";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    //Invalid signing region does not affect the operations.
    //Root directory will just take any value to use as root directory name, there cannot be an invalid name.

    @Test
    void testInvalidSignerType() {

        String invalidSignerType = "dummy";
        assertThrows(StorageClientCreationException.class,
            () -> new StorageClient(accessKey, secret, serviceEndpoint, signingRegion, invalidSignerType, bucket,
                rootDirectory));
    }

    @Test
    void testInvalidBucket() throws StorageClientCreationException {

        String invalidBucket = "dummy";
        StorageClient client = new StorageClient(accessKey, secret, serviceEndpoint, signingRegion, signerType,
            invalidBucket, rootDirectory);
        ReflectionTestUtils.setField(client, "s3Client", new AmazonS3Fake());

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
