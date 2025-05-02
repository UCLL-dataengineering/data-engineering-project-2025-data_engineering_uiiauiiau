# Added the imports
from azure.storage.blob import BlobServiceClient
import pandas as pd
from processor import Processor

# Writer class
# - Writes to the local /output folder.
# - Writes to Azure Blog Storage.
class Writer:
    def __init__(self, connection_string, container_name):
        self.connection_string = connection_string
        self.container_name = container_name
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    def write(self, df: pd.DataFrame, filename, output_path):     
        df.to_csv(output_path, index=False)

        # Upload to Azure Blob Storage
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=filename)
        with open(output_path, "rb") as data_file:
            blob_client.upload_blob(data_file, overwrite=True)
        print(f"☁️ Uploaded to Azure Blob Storage: {self.container_name}/{filename}")

if __name__ == "__main__":
    processor = Processor()
    processed_data = processor.process().get_processed_data()
    
    writer = Writer("DefaultEndpointsProtocol=https;AccountName=uiiauiiau;AccountKey=ZxKBlPoSrGjlXyHwFUQLe1l7Ps74FVGs4j27S2QBCeOtYnGO+be0020Krs37xlOFMaXiGQN23s4++ASt+O0Tpg==;EndpointSuffix=core.windows.net", "nashville")
    writer.write(processed_data, "processed_nashville_housing.csv", 'Nashville Batch Processing/original/output/processed_nashville_housing.csv')
