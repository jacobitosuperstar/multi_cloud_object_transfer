# Cloud transfer from S3 to Azure
# Positional arguments give me something. What if a stranger sees the function
# and doesn't understand what is happening??. Use them somewhere else, away
# from my sight.

# python packages
import os
# Third party packages
import requests
# AWS S3 packages
import boto3
from botocore.exceptions import ClientError
# Azure Blob Storage packages
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
)
# from azure.storage.blob import BlockBlobService, PublicAccess
# from azure.storage.blob.models import Blob


def s3_to_azure(
    *,
    aws_object_key: str,
    azure_storage_container_name: str,
    aws_access_key_id: str = os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key: str = os.environ.get("AWS_SECRET_ACCESS_KEY"),
    aws_storage_bucket_name: str = os.environ.get("AWS_STORAGE_BUCKET_NAME"),
    aws_public_object: bool = False,
    aws_url_expiration_time: int = 3600,
    azure_storage_account_name: str = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
    azure_storage_access_key: str = os.environ.get("AZURE_STORAGE_ACCESS_KEY"),
    azure_storage_connection_string: str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
    azure_storage_blob_name: str = None,
):
    """This function gets a file from S3 and transfer it to Azure Storage
    in a data stream, so no excesive memory is used beign local storage or
    in memory storage. At the moment we don't rename dinamically the objects in
    case they already exists in Azure, we function deletes and re-writes.

    params:
    aws_access_key_id: str -> AWS access key, it tries to take the one from the
    enviroment variables if nothing is put.

    aws_secret_access_key: str -> AWS S3 secret access key, it tries to take
    the one from the enviroment variables if nothing is put.

    aws_storage_bucket_name: str -> AWS S3 Bucket name.

    aws_object_key: str -> the key of the object that we are going to transfer.

    aws_public_object: bool -> if the object if public or not, to create a
    minimal URL in case it is.

    aws_url_expiration_time: int -> URL expiration time, according to the
    documentation is possible to create a valid URL up to 7 days, that are
    604800 seconds. Remmember that if the validation time expires before the
    download finishes you will only have a partial transfer.

    For the azure authentication is needed one of there, the
    azure_storage_account_name and the azure_storage_access_key or the
    azure_storage_connection_string, to be able to connect to the azure storage
    service.

    azure_storage_account_name: str -> This is the Windows Azure Storage
    Account name, which in many cases is also the first part of the url for
    instance: http://azure_storage_account_name.blob.core.windows.net/ would
    mean.

    azure_storage_access_key: str -> Key that gives us access to the account.

    azure_storage_connection_string: str -> If specified, this will override
    all other parameters.

    azure_storage_blob_name: str -> The destination name, if it happends to be
    None, it will be equal to the aws_object_key.
    """
    # accessing the AWS bucket
    try:
        aws_session = boto3.session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
    except Exception as e:
        print(e)
        return

    s3_client = aws_session.client('s3')

    if azure_storage_blob_name is None:
        azure_storage_blob_name = aws_object_key

    # generating the URLs for the S3 object
    if aws_public_object is True:
        # simple URL for public objects
        object_url = (
            'https://s3.amazonaws.com/'
            f'{aws_storage_bucket_name}/'
            f'{aws_object_key}/'
        )
    else:
        # try and catch error for the creation of the signed URL of the
        # uploaded file in case this file is private
        try:
            object_url = s3_client.generate_presigned_url(
                "get_object",
                params={
                    "Bucket": aws_storage_bucket_name,
                    "key": aws_object_key,
                },
                ExpiresIn=aws_url_expiration_time
            )
        except ClientError as e:
            print(e)
            return
    # accesing AzureStorage
    if azure_storage_connection_string is not None:
        # log on with the connection string
        try:
            blob_client = BlobClient.from_connection_string(
                conn_str=azure_storage_connection_string,
                container_name=azure_storage_container_name,
                blob_name=azure_storage_blob_name,
            )
        except Exception as e:
            print(e)
            return
    else:
        # log on with the account name and the access key
        try:
            account_url = (
                'https://'
                f'{azure_storage_account_name}'
                'blob.core.windows.net/'
            )
            blob_client = BlobServiceClient(
                account_url=account_url,
                credential=azure_storage_access_key,
            ).get_blob_client(
                container=azure_storage_container_name,
                blob=azure_storage_blob_name,
            )
        except Exception as e:
            print(e)
            return

    # checker and deleter existing blobs with the same name
    if blob_client.exists:
        blob_client.delete_blob()
        blob_client.create_append_blob()

    # creating the request for the requests package
    object_stream = requests.get(object_url, stream=True)
    with object_stream as stream:
        stream.raise_for_status()
        for chunk in stream.iter_content(chunck_size=1024):
            blob_client.append_block(chunk)
    return print(
        f"Finalized process for {azure_storage_blob_name}"
    )
