import sys
import asyncio
import uuid
import tempfile
from azure_blob_processor import Decompressor
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient


# constants
CONTAINER_NAME = "full-test"
CONNECT_STR_SECRET_NAME = "connectstr"

KEYVAULT_URL = "https://dhub-keyvault.vault.azure.net"

EVENTHUB_CONNECT_STR_SECRET_NAME = "eventhub-connectstr"

EVENTHUB_NAME = "test"


async def main():
    args = sys.argv[1:]

    raw_dir = f"{args[0]}/raw"
    list = args[0].split('/')
    curated_dir = f"{'/'.join(list)}/curated"

    print(f"raw_dir: {raw_dir}, curated_dir: {curated_dir}")

    local_temp_dir = "{}/{}".format(tempfile.gettempdir(), uuid.uuid4()) 

    credential = DefaultAzureCredential()
    async with credential:
        client = SecretClient(KEYVAULT_URL, credential)
        secret = await client.get_secret(CONNECT_STR_SECRET_NAME)
        eventhub_secret = await client.get_secret(EVENTHUB_CONNECT_STR_SECRET_NAME)
        await client.close()

    decompressor = Decompressor(secret.value, eventhub_secret.value, EVENTHUB_NAME)
    await decompressor.process_blob_files(CONTAINER_NAME, raw_dir, curated_dir, local_temp_dir)


if __name__ == "__main__":
    asyncio.run(main())