import os
from azure.storage.blob import BlobServiceClient
import pandas as pd
from processor import Processor

class Writer:
    def __init__(self, connection_string: str = "DefaultEndpointsProtocol=https;AccountName=uiiauiiau;AccountKey=ZxKBlPoSrGjlXyHwFUQLe1l7Ps74FVGs4j27S2QBCeOtYnGO+be0020Krs37xlOFMaXiGQN23s4++ASt+O0Tpg==;EndpointSuffix=core.windows.net", 
                 container_name: str = "nashville", local_output_folder: str = "output"):
        self.connection_string = connection_string
        self.container_name = container_name
        self.local_output_folder = local_output_folder

        os.makedirs(self.local_output_folder, exist_ok=True)
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    def write(self, df: pd.DataFrame, filename: str = "processed_nashville_housing.csv"):     
        output_path = 'output/processed_nashville_housing.csv'
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)

        # Upload to Azure Blob Storage
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=filename)
        with open("output\processed_nashville_housing.csv", "rb") as data_file:
            blob_client.upload_blob(data_file, overwrite=True)
        print(f"☁️ Uploaded to Azure Blob Storage: {self.container_name}/{filename}")

if __name__ == "__main__":
    processor = Processor()
    processed_data = processor.process().get_processed_data()
    
    writer = Writer()
    writer.write(processed_data)
