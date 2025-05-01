import pandas as pd
import numpy as np
import os
from datetime import datetime
from reader import Reader
from validator import Validator

class Processor:
    
    def __init__(self, proceed_with_errors=False):
        """Initialize the processor by reading and validating data"""
        self.data = Reader.read_last_file()
        if self.data is None:
            raise FileNotFoundError("No file found in the input directory.")

        # Validate the data
        validator = Validator(self.data)
        errors = validator.validate()
        
        # Print validation summary
        print("Validation Summary:")
        summary = validator.summary()
        for key, value in summary.items():
            print(f"{key}: {value}")
        
        # Show validation errors if present
        if errors:
            print("\nValidation errors found:")
            for i, error in enumerate(errors[:10]):
                print(error)
            if len(errors) > 10:
                print(f"...and {len(errors) - 10} more errors")
            
            # Either stop processing or continue with warnings based on the parameter
            if not proceed_with_errors:
                raise ValueError("Validation failed. Please fix the errors before processing.")
            else:
                print("WARNING: Proceeding with processing despite validation errors.")
                # Optionally filter out invalid rows when proceeding with errors
                self.data = validator.get_validated_data(filter_invalid=True)
                print(f"Using {len(self.data)} valid records for processing after filtering invalid rows")
        
        self.processed_data = None
    
    def _add_temperature_category(self):
        """Add temperature category based on temperature in Celsius"""
        # Check for the correct temperature column names from the validator schema
        temp_col = None
        if 'temperature_celsius' in self.data.columns:
            temp_col = 'temperature_celsius'
        elif 'temperature_fahrenheit' in self.data.columns:
            # Convert Fahrenheit to Celsius for consistent categorization
            self.data['temperature_celsius'] = (self.data['temperature_fahrenheit'] - 32) * 5/9
            temp_col = 'temperature_celsius'
        
        if temp_col:
            bins = [-60, 0, 10, 20, 30, 40, 60]
            labels = ['Freezing', 'Cold', 'Cool', 'Mild', 'Warm', 'Hot']
            self.data['temperature_category'] = pd.cut(self.data[temp_col], bins=bins, labels=labels)
        return self
    
    def _add_temperature_deviation(self):
        """Calculate deviation from mean temperature"""
        # Check for the correct temperature column names
        temp_col = None
        if 'temperature_celsius' in self.data.columns:
            temp_col = 'temperature_celsius'
        elif 'temperature_fahrenheit' in self.data.columns:
            temp_col = 'temperature_fahrenheit'
        
        if temp_col:
            mean_temp = self.data[temp_col].mean()
            self.data['temperature_deviation'] = self.data[temp_col] - mean_temp
        return self
    
    
    def _remove_duplicates(self):
        """Remove duplicate records from the dataset"""
        # Check for exact duplicates first
        original_count = len(self.data)
        self.data.drop_duplicates(inplace=True)
        duplicates_removed = original_count - len(self.data)
        
        # Check for location-date duplicates if those columns exist
        location_fields = {'location_name', 'country', 'latitude', 'longitude'}
        date_fields = {'last_updated', 'date'}
        
        # Find available columns for deduplication
        location_cols = [col for col in self.data.columns if col in location_fields]
        date_cols = [col for col in self.data.columns if col in date_fields]
        
        if location_cols and date_cols:
            before_dedup = len(self.data)
            self.data.drop_duplicates(subset=location_cols + date_cols, keep='first', inplace=True)
            location_date_dups = before_dedup - len(self.data)
            if location_date_dups > 0:
                print(f"Removed {location_date_dups} location-date duplicate entries")
        
        # Reset the index after removing duplicates
        self.data.reset_index(drop=True, inplace=True)
        
        if duplicates_removed > 0:
            print(f"Removed {duplicates_removed} exact duplicate records")
        return self

    
    def _add_air_quality_category(self):
        if 'air_quality_us-epa-index' in self.data.columns:
            conditions = [
                self.data['air_quality_us-epa-index'] == 1,
                self.data['air_quality_us-epa-index'] == 2,
                self.data['air_quality_us-epa-index'] == 3,
                self.data['air_quality_us-epa-index'] == 4,
                self.data['air_quality_us-epa-index'] == 5,
                self.data['air_quality_us-epa-index'] == 6
            ]
            choices = ['Good', 'Moderate', 'Unhealthy for Sensitive Groups', 
                      'Unhealthy', 'Very Unhealthy', 'Hazardous']
            self.data['air_quality_category'] = np.select(conditions, choices, default='Unknown')
        return self
    
    def process(self):
        """Execute all processing steps and return the processed data"""
        # Make a copy of the original data to avoid modifying it
        self.processed_data = self.data.copy()
        self.data = self.processed_data  # For method chaining to work with self.data
        
        # Apply all processing steps using method chaining
        (self
         ._add_temperature_category()
         ._add_temperature_deviation()
         ._add_air_quality_category()
         ._remove_duplicates()
        )
        
        # Update processed_data with the final result
        self.processed_data = self.data
        
        # Print summary of processing
        print(f"Processing complete. Result contains {len(self.processed_data)} rows and {len(self.processed_data.columns)} columns")
        return self.processed_data

    def get_processed_data(self):
        """Get the processed data or raise an error if processing hasn't been done"""
        if self.processed_data is None:
            raise ValueError("Data has not been processed. Call process() first.")
        return self.processed_data
    
    def save_processed_data(self, output_dir=None):
        """Save the processed data to the output directory"""
        if self.processed_data is None:
            raise ValueError("No processed data available to save. Call process() first.")
        
        if output_dir is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            output_dir = os.path.join(script_dir, "output")
        
        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"processed_weather_{timestamp}.csv")
        
        # Save to CSV
        self.processed_data.to_csv(output_file, index=False)
        print(f"Processed data saved to: {output_file}")
        return output_file

if __name__ == "__main__":
    try:
        # Initialize processor - set to True if you want to continue despite errors
        processor = Processor(proceed_with_errors=False) 
        
        # Process the data using method chaining
        processed_data = processor.process()
        
        if processed_data is not None:
            # Display info about the processed data
            print("\nProcessed Data Summary:")
            print(f"Shape: {processed_data.shape}")
            print(f"Columns: {list(processed_data.columns)}")
            print("\nData Info:")
            print(processed_data.info())
            
            # Save the processed data
            output_file = processor.save_processed_data()
            print(f"Output saved to: {output_file}")
        else:
            print("No data was processed. Cannot save output.")
        
    except ValueError as e:
        print(f"Processing terminated: {e}")
    except FileNotFoundError as fe:
        print(f"File error: {fe}")
    except Exception as ex:
        print(f"Unexpected error: {ex}")
        import traceback
        traceback.print_exc()