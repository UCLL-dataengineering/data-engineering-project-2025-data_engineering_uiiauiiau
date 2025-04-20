import pandas as pd

# Configure pandas to display all columns
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 500)

class Reader:
    def __init__(self, file_path=None):
        if file_path is None:
            self.file_path = 'input/Nashville_housing_data_2013_2016.csv'
        else:
            self.file_path = file_path
        self.data = None
        
    def load_data(self):                
        self.data = pd.read_csv(self.file_path)
        self.data = self.data.reset_index(drop=True)
        return self.data

# Execute the data loading when run as a script
if __name__ == "__main__":
    reader = Reader()
    data = reader.load_data()
    print(data.head(2))