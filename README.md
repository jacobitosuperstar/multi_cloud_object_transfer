# Multi Cloud Object Transfer

For work, I am in need of a tool to transfer objects between clouds, given the
specifications of each service, so I wanted to do a general tool, that people
can contribute or just search to see if they can adapt it to fit their personal
needs.

The need is to transfer files and not read them into memory fully, and not to
download them fully into the filesystem. Just blind transfer.

The package is tought to be sync only at the moment, but as we progress in it's
building, it will have an async version, but as this is much of waiting,
treatds are not the devil in this kind of processes.

Is a work in progress but you can contact me and ask me anything.

## How it works??

- we generate an url from which we can request the file.
- stream the response.
- each chunk is uploaded using the different functions to upload a file by
  chunks

## Services that I plan to get working

- AWS S3 to AzureStorageBlob
- AzureStorageBlob to AWS S3
- AWS S3 to gCloudStorage
- gCloudStorage to AWS S3
- AzureStorageBlob to gCloudStorage
- gCloudStorage to AzureStorageBlob

## Requierements

- requests
- azure-storage-blob
- boto3
- google-cloud-storage
