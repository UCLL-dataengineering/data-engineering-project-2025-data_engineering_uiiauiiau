import pandas as pd
import os
from pathlib import Path

class Reader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None
        
    def load_data(self):
        try:
            # Check if file exists
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"File not found: {self.file_path}")
            
            # Check file extension
            if not self.file_path.lower().endswith('.csv'):
                raise ValueError(f"File must be a CSV: {self.file_path}")
            
            # Read the CSV file
            self.data = pd.read_csv(self.file_path)
            return self.data
        except pd.errors.EmptyDataError:
            print(f"Error: The file {self.file_path} is empty")
            return None
        except pd.errors.ParserError:
            print(f"Error: Could not parse {self.file_path} as CSV")
            return None
        except Exception as e:
            print(f"Error reading file {self.file_path}: {str(e)}")
            return None
    
    @staticmethod
    def read_last_file():
        """
        Read the most recent file from the input directory based on modification time
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        input_dir = os.path.join(script_dir, "input")
        
        # Check if input directory exists
        if not os.path.exists(input_dir):
            print(f"Input directory does not exist: {input_dir}")
            return None
        
        # Get all CSV files in the input directory
        input_files = [
            f for f in Path(input_dir).glob("*.csv") 
            if os.path.isfile(os.path.join(input_dir, f))
        ]
        
        if not input_files:
            print("No CSV files found in the input directory")
            return None
        
        # Sort files by modification time (newest last)
        input_files.sort(key=lambda f: os.path.getmtime(os.path.join(input_dir, f)))
        
        # Get the most recent file
        last_file = input_files[-1]
        last_file_path = os.path.join(input_dir, last_file)
        
        print(f"Reading most recent file: {last_file}")
        
        # Create reader and load data
        reader = Reader(last_file_path)
        last_data = reader.load_data()
        
        return last_data

if __name__ == "__main__":
    last_data = Reader.read_last_file()
    if last_data is not None:
        print(f"Data shape: {last_data.shape}")
        print(f"Columns: {list(last_data.columns)}")
        print("\nFirst 5 rows:")
        print(last_data.head())
    else:
        print("Failed to load data.")