
import pandas as pd
import numpy as np
from datetime import datetime
from reader import Reader
import re


class Validator:

    def __init__(self, data=None):
        self.data = data
        self.validation_results = {
            'valid_records': 0,
            'invalid_records': 0,
            'validation_errors': []
        }
        
        # Define non-mandatory columns that won't trigger errors when missing
        self.non_mandatory_columns = [
            'Suite/ Condo   #', 'Owner Name', 'Address', 'City', 'State', 
            'Tax District', 'Foundation Type', 'Exterior Wall', 'Grade'
        ]
    
    def validate_dataset(self):
        if self.data is None:
            raise ValueError("No data loaded. Call load_data() first.")
            
        self.validation_results = {
            'valid_records': 0,
            'invalid_records': 0,
            'validation_errors': []
        }
        
        # Validate each record
        for index, record in self.data.iterrows():
            record_errors = self._validate_record(record, index)
            
            if record_errors:
                self.validation_results['invalid_records'] += 1
                self.validation_results['validation_errors'].extend(record_errors)
            else:
                self.validation_results['valid_records'] += 1
        
        return self
    
    def _validate_record(self, record, index):
        errors = []
        
        # Non-mandatory field validations
        
        # Suite/Condo (non-mandatory)
        if pd.notna(record.get('Suite/ Condo   #')):
            if not isinstance(record['Suite/ Condo   #'], (str, int, float)):
                errors.append(f"Record {index}: Suite/Condo should be string or numeric")
        
        # Validate Owner Name (non-mandatory)
        if pd.notna(record.get('Owner Name')):
            if not isinstance(record['Owner Name'], str):
                errors.append(f"Record {index}: Owner Name should be a string")
        
        # Validate Address (non-mandatory)
        if pd.notna(record.get('Address')):
            if not isinstance(record['Address'], str):
                errors.append(f"Record {index}: Address should be a string")
        
        # Validate City (non-mandatory)
        if pd.notna(record.get('City')):
            if not isinstance(record['City'], str):
                errors.append(f"Record {index}: City should be a string")
        
        # Validate State (non-mandatory)
        if pd.notna(record.get('State')):
            if not isinstance(record['State'], str):
                errors.append(f"Record {index}: State should be a string")
            elif len(record['State']) != 2:  # 2-letter state codes
                errors.append(f"Record {index}: State should be a 2-letter code")
        
        # Validate Tax District (non-mandatory)
        if pd.notna(record.get('Tax District')):
            if not isinstance(record['Tax District'], str):
                errors.append(f"Record {index}: Tax District should be a string")
        
        # Validate Foundation Type (non-mandatory)
        if pd.notna(record.get('Foundation Type')):
            if not isinstance(record['Foundation Type'], str):
                errors.append(f"Record {index}: Foundation Type should be a string")
            # Check if the value is valid (case-insensitive)
            elif not re.fullmatch(r'[A-Z ]+', record['Foundation Type'].strip()):
                errors.append(f"Record {index}: Foundation Type '{record['Foundation Type']}' is invalid (must contain only uppercase letters and spaces)")

        
        # Validate Exterior Wall (non-mandatory)
        if pd.notna(record.get('Exterior Wall')):
            if not isinstance(record['Exterior Wall'], str):
                errors.append(f"Record {index}: Exterior Wall should be a string")
            # Check if the value is valid (case-insensitive)
            elif not re.fullmatch(r'[A-Z/ ]+', record['Exterior Wall'].strip()):
                errors.append(f"Record {index}: Exterior Wall '{record['Exterior Wall']}' is invalid (must contain only uppercase letters, spaces, or slashes)")

        
        # Validate Grade (non-mandatory)
        if pd.notna(record.get('Grade')):
            if not isinstance(record['Grade'], str):
                errors.append(f"Record {index}: Grade should be a string")
            else:
                # Handle grades with spaces (e.g., 'B   ')
                grade_clean = record['Grade'].strip()
                if not re.fullmatch(r'[A-Z][+-]?', grade_clean):
                    errors.append(f"Record {index}: Grade '{record['Grade']}' is invalid (must be a single uppercase letter optionally followed by + or -)")

            
        # Validate fields used in calculations (if present)
        
        # Sale Price and Finished Area for price per square foot calculation
        if pd.notna(record.get('Sale Price')) and pd.notna(record.get('Finished Area')):
            try:
                float(record['Sale Price'])
            except (ValueError, TypeError):
                errors.append(f"Record {index}: Sale Price should be numeric")
                
            try:
                float(record['Finished Area'])
                if float(record['Finished Area']) == 0:
                    errors.append(f"Record {index}: Finished Area cannot be zero (division by zero in price per sqft)")
            except (ValueError, TypeError):
                errors.append(f"Record {index}: Finished Area should be numeric")
        
        # Year Built for property age calculation
        if pd.notna(record.get('Year Built')):
            try:
                year_built = float(record['Year Built'])
                if year_built < 1700 or year_built > datetime.now().year:
                    errors.append(f"Record {index}: Year Built ({year_built}) is outside reasonable range")
            except (ValueError, TypeError):
                errors.append(f"Record {index}: Year Built should be numeric")
        
        # Sale Date validation
        if pd.notna(record.get('Sale Date')):
            if isinstance(record['Sale Date'], str):
                try:
                    datetime.strptime(record['Sale Date'], '%Y-%m-%d')
                except ValueError:
                    errors.append(f"Record {index}: Sale Date should be in YYYY-MM-DD format")
        
        # Building Value for land-to-building ratio (avoid division by zero)
        if pd.notna(record.get('Building Value')):
            try:
                building_value = float(record['Building Value'])
                if building_value == 0:
                    errors.append(f"Record {index}: Building Value is zero (potential division by zero in ratio)")
            except (ValueError, TypeError):
                errors.append(f"Record {index}: Building Value should be numeric")
                
        return errors

    def get_validation_results(self):
        return self.validation_results
    
    def get_validation_summary(self):
        return {
            'total_records': self.validation_results['valid_records'] + self.validation_results['invalid_records'],
            'valid_records': self.validation_results['valid_records'],
            'invalid_records': self.validation_results['invalid_records'],
            'error_count': len(self.validation_results['validation_errors']),
        }
    
    def get_validated_data(self):

        return self.data


# Example usage
if __name__ == "__main__":
    # Example usage with reader
    reader = Reader()
    data = reader.load_data()
    
    # Initialize validator and validate data
    validator = Validator(data)
    validator.validate_dataset()
    
    # Print validation summary
    print("Validation Summary:")
    print(validator.get_validation_summary())
    
    # Print the first 5 validation errors if any
    errors = validator.get_validation_results()['validation_errors']
    if errors:
        print("\nSample validation errors (first 10):")
        for i, error in enumerate(errors[:10]):
            print(error)
        if len(errors) > 10:
            print(f"...and {len(errors) - 10} more errors")
    
    # Get validated data for processing
    validated_data = validator.get_validated_data()