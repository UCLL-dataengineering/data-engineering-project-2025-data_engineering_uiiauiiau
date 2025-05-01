import pandas as pd
import numpy as np
from datetime import datetime
import re
import logging
from scipy import stats
from processor import Processor


class BackupValidator:

    def __init__(self, processed_data=None):

        self.data = processed_data
        self.validation_results = {
            'valid_records': 0,
            'flagged_records': 0,
            'validation_flags': []
        }

        # Fields that should have been calculated during processing
        self.calculated_fields = [
            'Price per Square Foot',
            'Property Age',
            'Sale Year',
            'Sale Month',
            'Land-to-Building Ratio',
            'Sale Price Category',
            'Family Name',
            'First Name',
            'Owner Name'
        ]

    def validate(self):
        if self.data is None:
            raise ValueError("No data loaded. Call load_data() first.")

        # Reset validation results
        self.validation_results = {
            'valid_records': 0,
            'flagged_records': 0,
            'validation_flags': []
        }

        # Check for required calculated fields
        self._validate_calculated_fields()

        # Validate calculated values
        self._validate_calculations()

        # Count valid records
        self.validation_results['valid_records'] = len(self.data) - self.validation_results['flagged_records']

        return self

    def _validate_calculated_fields(self):
        missing_fields = [field for field in self.calculated_fields if field not in self.data.columns]
        
        if missing_fields:
            for field in missing_fields:
                self.validation_results['validation_flags'].append(
                    f"Calculated field '{field}' is missing from processed data"
                )
            self.validation_results['flagged_records'] += 1

    def _validate_calculations(self):
        # Check Price per Square Foot calculation
        if all(field in self.data.columns for field in ['Price per Square Foot', 'Sale Price', 'Finished Area']):
            # Calculate expected values
            expected_price_per_sqft = self.data['Sale Price'] / self.data['Finished Area']
            
            # Find records with incorrect calculations (allowing for small rounding differences)
            incorrect = self.data[
                (self.data['Price per Square Foot'].notna()) & 
                (abs(self.data['Price per Square Foot'] - expected_price_per_sqft) > 0.01)
            ]
            
            if len(incorrect) > 0:
                for idx, row in incorrect.iterrows():
                    expected = row['Sale Price'] / row['Finished Area']
                    self.validation_results['validation_flags'].append(
                        f"Record {idx}: Price per Square Foot calculation is incorrect. Got {row['Price per Square Foot']}, expected {expected:.2f}"
                    )
                self.validation_results['flagged_records'] += len(incorrect)

        # Check Property Age calculation
        if all(field in self.data.columns for field in ['Property Age', 'Sale Date', 'Year Built']):
            # Calculate expected values
            if not pd.api.types.is_datetime64_any_dtype(self.data['Sale Date']):
                sale_years = pd.to_datetime(self.data['Sale Date']).dt.year
            else:
                sale_years = self.data['Sale Date'].dt.year
            
            expected_property_age = sale_years - self.data['Year Built']
            
            # Find records with incorrect calculations
            incorrect = self.data[
                (self.data['Property Age'].notna()) & 
                (self.data['Property Age'] != expected_property_age)
            ]
            
            if len(incorrect) > 0:
                for idx, row in incorrect.iterrows():
                    if pd.api.types.is_datetime64_any_dtype(row['Sale Date']):
                        sale_year = row['Sale Date'].year
                    else:
                        sale_year = pd.to_datetime(row['Sale Date']).year
                    expected = sale_year - row['Year Built']
                    self.validation_results['validation_flags'].append(
                        f"Record {idx}: Property Age calculation is incorrect. Got {row['Property Age']}, expected {expected}"
                    )
                self.validation_results['flagged_records'] += len(incorrect)

        # Check Sale Price Category calculation
        if 'Sale Price Category' in self.data.columns:
            # Check Low category
            incorrect_low = self.data[
                (self.data['Sale Price'] < 100000) & 
                (self.data['Sale Price Category'] != 'Low')
            ]
            
            # Check Medium category
            incorrect_medium = self.data[
                (self.data['Sale Price'] >= 100000) & 
                (self.data['Sale Price'] <= 300000) & 
                (self.data['Sale Price Category'] != 'Medium')
            ]
            
            # Check High category
            incorrect_high = self.data[
                (self.data['Sale Price'] > 300000) & 
                (self.data['Sale Price Category'] != 'High')
            ]
            
            total_incorrect = len(incorrect_low) + len(incorrect_medium) + len(incorrect_high)
            
            if total_incorrect > 0:
                for idx, row in pd.concat([incorrect_low, incorrect_medium, incorrect_high]).iterrows():
                    expected = 'Low' if row['Sale Price'] < 100000 else 'Medium' if row['Sale Price'] <= 300000 else 'High'
                    self.validation_results['validation_flags'].append(
                        f"Record {idx}: Sale Price Category incorrect. Got '{row['Sale Price Category']}', expected '{expected}' for price {row['Sale Price']}"
                    )
                self.validation_results['flagged_records'] += total_incorrect

    def get_validation_results(self):
        return self.validation_results

    def get_validation_summary(self):
        return {
            'total_records': len(self.data) if self.data is not None else 0,
            'valid_records': self.validation_results['valid_records'],
            'flagged_records': self.validation_results['flagged_records'],
            'flag_count': len(self.validation_results['validation_flags'])
        }

    def get_flagged_records(self):
        if self.data is None:
            return pd.DataFrame()
            
        # Extract unique record indices from validation flags
        flag_pattern = r"Record (\d+):"
        flagged_indices = set()
        
        for flag in self.validation_results['validation_flags']:
            match = re.search(flag_pattern, flag)
            if match:
                try:
                    flagged_indices.add(int(match.group(1)))
                except ValueError:
                    continue
        
        # Return dataframe with only flagged records
        return self.data.loc[list(flagged_indices)]


# Example usage
if __name__ == "__main__":

    processor = Processor()
    processor.process()
    processed_data = processor.get_processed_data()
    
    # Initialize backup validator with processed data
    backup_validator = BackupValidator(processed_data=processed_data)
    backup_validator.validate()
    
    # Print validation summary
    print("Backup Validation Summary:")
    print(backup_validator.get_validation_summary())
    
    # Print the first 10 validation flags if any
    flags = backup_validator.get_validation_results()['validation_flags']
    if flags:
        print("\nSample validation flags (first 10):")
        for i, flag in enumerate(flags[:10]):
            print(flag)
        if len(flags) > 10:
            print(f"...and {len(flags) - 10} more flags")