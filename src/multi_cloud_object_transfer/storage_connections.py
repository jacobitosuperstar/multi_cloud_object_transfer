"""
Modulo to connect to Azure Storage
"""
# python packages
# Third party packages
# AWS S3 packages
import boto3
from botocore.exceptions import ClientError
# Azure Blob Storage packages
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
)


def get_azure_blob_client(
    *,
    azure_storage_container_name: str,
    azure_storage_blob_name: str,
    azure_storage_account_name: str = None,
    azure_storage_access_key: str = None,
    azure_storage_connection_string: str = None,
) -> BlobClient:
    """Function to generate a BlobClient for a file.

    params:
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

    return blob_client


def get_s3_client(
    *,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_storage_bucket_name: str,
):
    """This function return the client session in S3

    params:
    aws_access_key_id: str -> AWS access key, it tries to take the one from the
    enviroment variables if nothing is put.

    aws_secret_access_key: str -> AWS S3 secret access key, it tries to take
    the one from the enviroment variables if nothing is put.

    aws_storage_bucket_name: str -> AWS S3 Bucket name.
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
    return s3_client
