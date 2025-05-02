# Added the imports
import os
import datetime
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
        # Create a unique filename with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_base, filename_ext = os.path.splitext(filename)
        unique_filename = f"{filename_base}_{timestamp}{filename_ext}"
        unique_output_path = os.path.join(os.path.dirname(output_path), unique_filename)
        
        # Save to local file
        df.to_csv(unique_output_path, index=False)

        # Upload to Azure Blob Storage
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=unique_filename)
        with open(unique_output_path, "rb") as data_file:
            blob_client.upload_blob(data_file, overwrite=True)
        print(f"☁️ Uploaded to Azure Blob Storage: {self.container_name}/{unique_filename}")
        
        return unique_output_path, unique_filename

if __name__ == "__main__":
    processor = Processor()
    processed_data = processor.process().get_processed_data()
    
    writer = Writer("DefaultEndpointsProtocol=https;AccountName=uiiauiiau;AccountKey=ZxKBlPoSrGjlXyHwFUQLe1l7Ps74FVGs4j27S2QBCeOtYnGO+be0020Krs37xlOFMaXiGQN23s4++ASt+O0Tpg==;EndpointSuffix=core.windows.net", "weather")
    writer.write(processed_data, "processed_weather.csv", 'Weather Real-Time Processing/output/processed_weather.csv')
