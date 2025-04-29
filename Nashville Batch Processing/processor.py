import pandas as pd
import numpy as np
from datetime import datetime
from reader import Reader

class Processor:
    
    def __init__(self):
        reader = Reader()
        self.data = reader.load_data()
        
    # Remove all rows containing a missing value in a mandatory column.    
    def remove_rows_with_missing_mandatory_values(self):
        #Non-mandatory columns: ‘Suite/Condo’, ‘Owner Name’, ‘Adress’, ‘City’, ‘State’, ‘Tax District’,
        #‘Image’, ‘Foundation Type’, ‘Exterior Wall’, ‘Grade’
        mandatory_columns = ['Parcel ID', 'Land Use', 'Property Address', 'Property City',
                              'Sale Date', 'Sale Price', 'Legal Reference', 'Sold As Vacant', 'Multiple Parcels Involved in Sale'
                              , 'Acreage', 'Neighborhood', 'Land Value',
                                  'Building Value', 'Total Value', 'Finished Area', 'Year Built', 'Bedrooms', 'Full Bath', 'Half Bath']
        self.data = self.data.dropna(subset=mandatory_columns)
        missing_values = self.data[mandatory_columns].isnull().any(axis=1)

        
        
        if missing_values.any():
            missing_indices = self.data[missing_values].index.tolist()
            raise ValueError(f"Missing values found in mandatory columns at rows: {missing_indices}")
        
        return self
        
    # Remove columns 'image', 'Sold As Vacant' and 'Multiple Parcels Involved in Sale'.
    def remove_columns(self):
        columns_to_remove = ['image', 'Sold As Vacant', 'Multiple Parcels Involved in Sale']
        existing_columns = [col for col in columns_to_remove if col in self.data.columns]
        self.data = self.data.drop(columns=existing_columns)
        return self
        
    # Price per square foot.
    def add_price_per_sqft(self):
        self.data['Price per Square Foot'] = self.data['Sale Price'] / self.data['Finished Area']
        return self
    
    # Age of property.
    def add_property_age(self):
        if not pd.api.types.is_datetime64_any_dtype(self.data['Sale Date']):
            self.data['Sale Date'] = pd.to_datetime(self.data['Sale Date'])
            
        sale_years = self.data['Sale Date'].dt.year
        
        self.data['Property Age'] = sale_years - self.data['Year Built']
        return self
        
    # Sale year and sale month.
    def add_sale_year_month(self):
        if not pd.api.types.is_datetime64_any_dtype(self.data['Sale Date']):
            self.data['Sale Date'] = pd.to_datetime(self.data['Sale Date'])
            
        self.data['Sale Year'] = self.data['Sale Date'].dt.year
        self.data['Sale Month'] = self.data['Sale Date'].dt.month
        return self
        
    # Land-to-building value ratio.
    def add_land_building_ratio(self):
        self.data['Land-to-Building Ratio'] = self.data['Land Value'] / self.data['Building Value'].replace(0, np.nan)
        return self
     
    # Sale price category: Low (< 100 000), Medium (100 000 - 300 000), High (>300 000).   
    def add_price_category(self):
        conditions = [
            self.data['Sale Price'] < 100000,
            (self.data['Sale Price'] >= 100000) & (self.data['Sale Price'] <= 300000),
            self.data['Sale Price'] > 300000
        ]
        choices = ['Low', 'Medium', 'High']
        self.data['Sale Price Category'] = np.select(conditions, choices, default='Unknown')
        return self
    
    
    # Family Name and First name of owner (first person listed).
    def extract_owner_names(self):
        def extract_names(owner_string):
            if pd.isna(owner_string):
                return pd.Series([np.nan, np.nan])
                
            owner_string = str(owner_string).strip()
            
            if '&' in owner_string:
                owner_string = owner_string.split('&')[0].strip()
            if ',' in owner_string:
                parts = owner_string.split(',', 1)
                family_name = parts[0].strip()
                first_name = parts[1].strip() if len(parts) > 1 else np.nan
            else:
                parts = owner_string.split()
                if len(parts) > 1:
                    first_name = parts[0].strip()
                    family_name = ' '.join(parts[1:]).strip()
                else:
                    family_name = owner_string
                    first_name = np.nan
                    
            return pd.Series([family_name, first_name])
            
        self.data[['Family Name', 'First Name']] = self.data['Owner Name'].apply(extract_names)
        return self
    
    # Process all the clean up functions.
    def process(self):
        self.remove_rows_with_missing_mandatory_values()
        self.remove_columns()
        self.add_price_per_sqft()
        self.add_property_age()
        self.add_sale_year_month()
        self.add_land_building_ratio()
        self.add_price_category()
        self.extract_owner_names()
        self.data = self.data.reset_index(drop=True)
        return self
    
    # To show the processed data.   
    def get_processed_data(self):
        return self.data

# Execute the processing steps when run as a script
if __name__ == "__main__":
    processor = Processor()
    processor.process()
    processed_data = processor.get_processed_data()
    print(processed_data.info())