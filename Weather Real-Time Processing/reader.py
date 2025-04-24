import pandas as pd
import os

class Reader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None
        
    def load_data(self):                
        self.data = pd.read_csv(self.file_path)
        return self.data
    
    @staticmethod
    def read_last_file():
        """Read the last file in the input directory"""
        # Get list of files in the input directory
        input_files = os.listdir("input")
        
        if input_files:
            # Get the last file in the directory
            last_file = input_files[-1]
            last_file_path = os.path.join("input", last_file)
            
            reader = Reader(last_file_path)
            last_data = reader.load_data()
            
            print(f"Read last file: {last_file}")
            return last_data
        else:
            print("No files found in the input directory")
            return None

# If this script is run directly, read the last file
if __name__ == "__main__":
    last_data = Reader.read_last_file()
    if last_data is not None:
        print(f"Data shape: {last_data.shape}")
        print(f"Columns: {list(last_data.columns)}")
        print("\nFirst 5 rows:")
        print(last_data.head())