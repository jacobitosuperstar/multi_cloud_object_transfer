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
) -> dict:
    """This function gets an existing file from S3 and transfer it to Azure
    Storage in a data stream, so no excesive memory is used beign local storage
    or in memory storage, just bandwith.

    :aws_access_key_id: str -> AWS access key, it tries to take the one from the
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

    For the azure authentication is needed, the azure_storage_account_name
    and the azure_storage_access_key or the azure_storage_connection_string,
    to be able to connect to the azure storage service.

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
    information: dict -> Returns a dict with this structure
    {
        "azure_storage_container_name": str,
        "azure_storage_blob_name": str,
        "aws_storage_bucket_name": str,
        "aws_storage_object_key": str,
        "on_delete_transfer": bool,
    }
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

    # creating the request for the requests package
    object_stream = requests.get(object_url, stream=True)
    blob_client.upload_blob(object_stream)
    print(
        "Finalized process for: \n"
        f"Azure Storage Container: {azure_storage_container_name} \n"
        f"Azure Blob Name: {azure_storage_blob_name} \n"
    )
    # deleting the original object if conditional
    if aws_delete_after_transfer is True:
        s3_client.delete_object(
            Bucket=aws_storage_bucket_name,
            Key=aws_object_key,
        )
        print(
            "Object deleted from origin: \n"
            f"S3 Object Key: {aws_object_key} \n"
            f"S3 Bucket Name: {aws_storage_bucket_name} \n"
        )
    information = {
        "azure_storage_container_name": azure_storage_container_name,
        "azure_storage_blob_name": azure_storage_blob_name,
        "aws_storage_bucket_name": aws_storage_bucket_name,
        "aws_storage_object_key": aws_object_key,
    }
    return information


def azure_to_s3(
    *,
    azure_storage_blob_name: str,
    azure_storage_container_name: str,
    azure_storage_account_name: str = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
    azure_storage_access_key: str = os.environ.get("AZURE_STORAGE_ACCESS_KEY"),
    azure_storage_connection_string: str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
    azure_storage_blob_url_expiration_time: int = 3600,
    azure_storage_delete_after_transfer: bool = False,
    aws_object_key: str = None,
    aws_access_key_id: str = os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key: str = os.environ.get("AWS_SECRET_ACCESS_KEY"),
    aws_storage_bucket_name: str = os.environ.get("AWS_STORAGE_BUCKET_NAME"),
    aws_public_object: bool = False,
    aws_storage_key_overwrite: bool = False,
) -> dict:
    """This function gets an existing file from Azure and transfer it to AWS S3
    in a data stream, so no excesive memory is used beign local storage or in
    memory storage, just bandwith.

    For the azure authentication is always needed, the
    azure_storage_account_name and the azure_storage_access_key. You can put
    the azure_storage_connection_string, if desired, but isn't required. If
    None is given to each paremeter it will try to get them from the
    enviroment.

    :type azure_storage_blob_name: str
    :param azure_storage_blob_name =>

    :type azure_storage_container_name: str
    :param azure_storage_container_name =>

    :type azure_storage_account_name: str
    :param azure_storage_account_name => This is the Windows Azure Storage
    Account name, which in many cases is also the first part of the url for
    instance: https://azure_storage_account_name.blob.core.windows.net/

    :type azure_storage_access_key: str
    :param azure_storage_access_key => Azure access key. If None is given, it
    will try to take it from the enviroment.

    :type azure_storage_connection_string: str
    :param azure_storage_connection_string => Azure storage connection string.
    If None is given, it will to take it from the enviroment.

    :type azure_storage_blob_url_expiration_time: int
    :param azure_storage_blob_url_expiration_time: int => URL expiration time
    in seconds. Please check the documentation regarding the best practices
    when using SAS regarding URL duration, because you can make it eternal if
    you desire, but don't do it so that your information isn't compromised. The
    default time will be 3600 seconds. The generated URL will always use https.

    :type azure_storage_delete_after_transfer: bool
    :param azure_storage_delete_after_transfer => Checker to delete the
    original object after the transfer is done. The default is False.

    :type aws_object_key: str
    :param aws_object_key => The name of the object that will be uploaded to
    S3. If None is given, it will take the same name as the object in Azure
    Storage.

    :type aws_access_key_id: str
    :param aws_access_key_id => AWS access key. It tries to take it from the
    enviroment if none is given.

    :type aws_secret_access_key: str
    :param aws_secret_access_key => AWS secret access key. It tries to take it
    from the enviroment if none is given.

    :type aws_storage_bucket_name: str
    :param aws_storage_bucket_name => AWS storage bucket name. It tries to take
    it from the enviroment if none is given.

    :type aws_public_object: bool
    :param aws_public_object => Checker to see if the uploaded object will be
    public or not. The default value is False.

    :type aws_storage_key_overwrite: bool
    :param aws_storage_key_overwrite => Checker to see if rewritting an object
    with the same name is permited or not. The default is False.

    : type information: dict
    :return: information: dict -> Returns a dict with this structure:

        {
            "aws_storage_bucket_name": str,
            "aws_storage_object_key": str,
            "azure_storage_container_name": str,
            "azure_storage_blob_name": str,
            "delete_origin": bool,
        }

    """
    # accesing AzureStorage
    blob_client = get_azure_blob_client(
        azure_storage_container_name=azure_storage_container_name,
        azure_storage_blob_name=azure_storage_blob_name,
        azure_storage_account_name=azure_storage_account_name,
        azure_storage_access_key=azure_storage_access_key,
        azure_storage_connection_string=azure_storage_connection_string,
    )

    if aws_object_key is None:
        aws_object_key = azure_storage_blob_name
    # accessing the AWS bucket
    s3_client = get_s3_client(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_storage_bucket_name=aws_storage_bucket_name,
    )
    # deleting origin file from azure storage
    if azure_storage_delete_after_transfer is True:
        if blob_client.exists():
            blob_client.delete_blob()
    information = {
        "aws_storage_bucket_name": aws_storage_bucket_name,
        "aws_storage_object_key": aws_object_key,
        "azure_storage_container_name": azure_storage_container_name,
        "azure_storage_blob_name": azure_storage_blob_name,
    }
    return information
