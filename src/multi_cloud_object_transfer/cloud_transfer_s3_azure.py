"""Cloud transfer from S3 to Azure

Positional arguments give me something. What if a stranger sees the function
and doesn't understand what is happening??. Use them somewhere else, away
from my sight.
"""

# python packages
import os
# Third party packages
import requests
# AWS S3 packages
import boto3
from botocore.exceptions import ClientError
# Azure Blob Storage packages
# from azure.storage.blob import BlockBlobService, PublicAccess
# from azure.storage.blob.models import Blob
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
)
# Local Imports
from .utils import id_generator
from .storage_connections import (
    get_s3_client,
    get_azure_blob_client,
)


def s3_to_azure(
    *,
    aws_object_key: str,
    azure_storage_container_name: str,
    aws_access_key_id: str = os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key: str = os.environ.get("AWS_SECRET_ACCESS_KEY"),
    aws_storage_bucket_name: str = os.environ.get("AWS_STORAGE_BUCKET_NAME"),
    aws_public_object: bool = False,
    aws_url_expiration_time: int = 3600,
    aws_delete_after_transfer: bool = False,
    azure_storage_account_name: str = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
    azure_storage_access_key: str = os.environ.get("AZURE_STORAGE_ACCESS_KEY"),
    azure_storage_connection_string: str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
    azure_storage_blob_name: str = None,
    azure_storage_blob_overwrite: bool = False,
) -> str:
    """This function gets an existing file from S3 and transfer it to Azure
    Storage in a data stream, so no excesive memory is used beign local storage
    or in memory storage, just bandwith.

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

    aws_delete_after_transfer: bool -> Boolean to check if the original file
    will be deleted or not. By default is False.

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

    azure_storage_blob_overwrite: bool -> If there is already a blob with the
    azure_storage_blob_name, the original file could be re-written or and ID is
    generated that will differentiate the names of the blobs. By default the
    files aren't overwritten.

    returns:
    azure_storage_blob_name: str -> Returns the key of the file transfered to
    S3
    """
    # accessing the AWS bucket
    s3_client = get_s3_client(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_storage_bucket_name=aws_storage_bucket_name,
    )

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
    blob_client = get_azure_blob_client(
        azure_storage_container_name=azure_storage_container_name,
        azure_storage_blob_name=azure_storage_blob_name,
        azure_storage_account_name=azure_storage_account_name,
        azure_storage_access_key=azure_storage_access_key,
        azure_storage_connection_string=azure_storage_connection_string,
    )
    # checker and deleter existing blobs with the same name
    # if overwrite is true, we delete the possible coincidence and create a new
    # blob
    if azure_storage_blob_overwrite is True:
        if blob_client.exists():
            blob_client.delete_blob()
    # if overwrite is false, we generate and ID to put on the name and we
    # change it until the uniqueness condition is meet
    if azure_storage_blob_overwrite is False:
        original_name = azure_storage_blob_name
        exists = blob_client.exists()
        while exists is True:
            file_name, file_extension = os.path.splitext(original_name)
            random_id = id_generator()
            azure_storage_blob_name = (
                f"{file_name}"
                "_"
                f"{random_id}"
                f"{file_extension}"
            )
            blob_client = get_azure_blob_client(
                azure_storage_container_name=azure_storage_container_name,
                azure_storage_blob_name=azure_storage_blob_name,
                azure_storage_account_name=azure_storage_account_name,
                azure_storage_access_key=azure_storage_access_key,
                azure_storage_connection_string=azure_storage_connection_string,
            )
            exists = blob_client.exists()
    blob_client.create_append_blob()

    # creating the request for the requests package
    object_stream = requests.get(object_url, stream=True)
    with object_stream as stream:
        stream.raise_for_status()
        for chunk in stream.iter_content(chunck_size=1024):
            blob_client.append_block(chunk)
    print(
        f"Finalized process for {azure_storage_blob_name}"
    )
    return azure_storage_blob_name


def azure_to_s3(
    *,
    azure_storage_container_name: str,
    azure_storage_blob_name: str = None,
    azure_storage_account_name: str = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
    azure_storage_access_key: str = os.environ.get("AZURE_STORAGE_ACCESS_KEY"),
    azure_storage_connection_string: str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
    azure_storage_delete_after_transfer: bool = False,
    aws_access_key_id: str = os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key: str = os.environ.get("AWS_SECRET_ACCESS_KEY"),
    aws_storage_bucket_name: str = os.environ.get("AWS_STORAGE_BUCKET_NAME"),
    aws_public_object: bool = False,
    aws_object_key: str = None,
    aws_object_overwrite: bool = False,
):
    return aws_object_key
