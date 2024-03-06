# MERLOT S3 Library

This repository contains a maven library that abstracts interaction with S3 Buckets for data storage and retrieval.

## Structure

```
├── src/main/java/eu/merloteducation/s3library
│   ├── service    # provides Spring services for interacting with a configured S3 bucket
```

## Use the Library

To use this library you have to configure following properties in the application.yml:

    s3-library:
      access-key: [ACCESSKEY]
      secret: [SECRET]
      service-endpoint: [SERVICEENDPOINT]
      signing-region: [SIGNINGREGION, e.g. "de"]
      signer-type: [SIGNERTYPE, e.g. "S3SignerType"]
      bucket: [BUCKET]
      root-directory: [ROOTDIRECTORY]


## Run Tests inside StorageClientTest

You may want to run the tests inside StorageClientTest. For this edit the run configurations and add
following environment variables with valid values:

1. S3LIBRARY_ACCESSKEY
2. S3LIBRARY_SECRET
3. S3LIBRARY_SERVICEENDPOINT