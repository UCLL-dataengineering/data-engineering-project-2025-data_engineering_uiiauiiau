import pandas as pd
from datetime import datetime
from reader import Reader

class Validator:
    def __init__(self, data):
        self.data = data
        self.errors = []
        self.error_rows = set()  # Track rows with errors for filtering

        self.valid_moon_phases = {
            "new moon",
            "waxing crescent",
            "first quarter",
            "waxing gibbous",
            "full moon",
            "waning gibbous",
            "third quarter",
            "waning crescent",
            "last quarter"
        }

        # Define expected types/formats for each field
        self.schema = {
            'country': str,
            'location_name': str,
            'latitude': (float, int),
            'longitude': (float, int),
            'timezone': str,
            'last_updated_epoch': (float, int),
            'last_updated': 'datetime',
            'temperature_celsius': (float, int),
            'temperature_fahrenheit': (float, int),
            'condition_text': str,
            'wind_mph': (float, int),
            'wind_kph': (float, int),
            'wind_degree': (float, int),
            'wind_direction': str,
            'pressure_mb': (float, int),
            'pressure_in': (float, int),
            'precip_mm': (float, int),
            'precip_in': (float, int),
            'humidity': (float, int),
            'cloud': (float, int),
            'feels_like_celsius': (float, int),
            'feels_like_fahrenheit': (float, int),
            'visibility_km': (float, int),
            'visibility_miles': (float, int),
            'uv_index': (float, int),
            'gust_mph': (float, int),
            'gust_kph': (float, int),
            'air_quality_Carbon_Monoxide': (float, int),
            'air_quality_Ozone': (float, int),
            'air_quality_Nitrogen_dioxide': (float, int),
            'air_quality_Sulphur_dioxide': (float, int),
            'air_quality_PM2.5': (float, int),
            'air_quality_PM10': (float, int),
            'air_quality_us-epa-index': (float, int),
            'air_quality_gb-defra-index': (float, int),
            'sunrise': 'time',
            'sunset': 'time',
            'moonrise': 'time',
            'moonset': 'time',
            'moon_phase': str,
            'moon_illumination': int
        }

    def validate(self):
        """Validate the data according to the schema"""
        self.errors = []
        self.error_rows = set()
        
        # Check that all required columns are present
        missing_columns = set(self.schema.keys()) - set(self.data.columns)
        if missing_columns:
            self.errors.append(f"Missing columns in data: {', '.join(missing_columns)}")
            # If critical columns are missing, return early
            if len(missing_columns) > len(self.schema) / 2:  # If more than half the columns are missing
                return self.errors

        # Validate each row
        for i, row in self.data.iterrows():
            row_has_error = False
            
            for col, expected_type in self.schema.items():
                # Skip columns that don't exist in the dataset
                if col not in self.data.columns:
                    continue
                    
                value = row[col]

                # Check presence (mandatory)
                if pd.isna(value):
                    self.errors.append(f"Record {i}: Missing value in '{col}'")
                    row_has_error = True
                    continue

                # Moon phase validation
                if col == 'moon_phase':
                    # Case-insensitive comparison
                    if str(value).strip().lower() not in self.valid_moon_phases:
                        self.errors.append(f"Record {i}: '{col}' contains invalid moon phase '{value}'")
                        row_has_error = True
                    continue  # Skip further type checking for this field

                # Check datetime format
                if expected_type == 'datetime':
                    try:
                        datetime.strptime(str(value), '%Y-%m-%d %H:%M')
                    except ValueError:
                        self.errors.append(f"Record {i}: '{col}' is not in 'YYYY-MM-DD HH:MM' format")
                        row_has_error = True
                    continue

                # Check time format
                elif expected_type == 'time':
                    if isinstance(value, str) and value.strip().lower() in {
                        "no moonset", "no sunrise", "no sunset", "no moonrise"
                    }:
                        continue  # Valid exception
                    try:
                        datetime.strptime(str(value).strip(), '%I:%M %p')
                    except ValueError:
                        self.errors.append(f"Record {i}: '{col}' is not in 'HH:MM AM/PM' format or a valid exception")
                        row_has_error = True
                    continue

                # Generic type check (float/int/str)
                else:
                    if not isinstance(value, expected_type):
                        try:
                            # Attempt conversion to check if the value is convertible
                            if isinstance(expected_type, tuple):
                                # Try each type in the tuple
                                conversion_success = False
                                for t in expected_type:
                                    try:
                                        t(value)
                                        conversion_success = True
                                        break
                                    except (ValueError, TypeError):
                                        pass
                                if not conversion_success:
                                    self.errors.append(f"Record {i}: '{col}' is not of type {expected_type}")
                                    row_has_error = True
                            else:
                                expected_type(value)  # Attempt conversion
                        except (ValueError, TypeError):
                            self.errors.append(f"Record {i}: '{col}' is not of type {expected_type.__name__}")
                            row_has_error = True
            
            if row_has_error:
                self.error_rows.add(i)

        return self.errors

    def summary(self):
        """Return a summary of validation results"""
        total = len(self.data)
        invalid = len(self.error_rows)
        return {
            'total_records': total,
            'invalid_records': invalid,
            'error_count': len(self.errors),
            'valid_percentage': round((total - invalid) / total * 100, 2) if total > 0 else 0
        }
    
    def get_validated_data(self, filter_invalid=False):
        """
        Return the validated data
        If filter_invalid is True, rows with validation errors are removed
        """
        if filter_invalid and self.error_rows:
            # Return only valid rows
            valid_indices = list(set(range(len(self.data))) - self.error_rows)
            return self.data.iloc[valid_indices].copy().reset_index(drop=True)
        return self.data

if __name__ == "__main__":
    # Load the most recent CSV file from the input folder
    data = Reader.read_last_file()
    
    if data is not None:
        print(f"\nLoaded data with shape: {data.shape}")
        print(f"Columns: {list(data.columns)}\n")

        # Initialize and run the validator
        validator = Validator(data)
        errors = validator.validate()

        # Print summary
        print("Validation Summary:")
        summary = validator.summary()
        for key, value in summary.items():
            print(f"{key}: {value}")

        # Show a few sample errors if present
        if errors:
            print("\nSample validation errors (first 10):")
            for i, error in enumerate(errors[:10]):
                print(error)
            if len(errors) > 10:
                print(f"...and {len(errors) - 10} more errors")
            
            # Get clean data
            valid_data = validator.get_validated_data(filter_invalid=True)
            print(f"\nAfter filtering invalid records: {valid_data.shape} rows remain")
        else:
            print("\nNo validation errors found. All data is valid!")
    else:
        print("No data to validate.")