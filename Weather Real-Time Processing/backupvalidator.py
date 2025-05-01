import pandas as pd
import numpy as np
import re
from processor import Processor


class BackupValidator:

    def __init__(self, processed_data=None):
        self.data = processed_data
        self.validation_results = {
            'valid_records': 0,
            'flagged_records': 0,
            'validation_flags': []
        }

        self.expected_fields = [
            'temperature_category',
            'temperature_deviation',
            'air_quality_category'
        ]

    def validate(self):
        if self.data is None:
            raise ValueError("No data provided for validation.")
        
        self.validation_results = {
            'valid_records': 0,
            'flagged_records': 0,
            'validation_flags': []
        }

        self._validate_fields_exist()
        self._validate_temperature_category()
        self._validate_temperature_deviation()
        self._validate_air_quality_category()

        self.validation_results['valid_records'] = len(self.data) - self.validation_results['flagged_records']
        return self

    def _validate_fields_exist(self):
        missing = [field for field in self.expected_fields if field not in self.data.columns]
        for field in missing:
            self.validation_results['validation_flags'].append(
                f"Calculated field '{field}' is missing from processed data."
            )
        self.validation_results['flagged_records'] += len(missing)

    def _validate_temperature_category(self):
        if 'temperature_category' in self.data.columns and 'temperature_celsius' in self.data.columns:
            bins = [-60, 0, 10, 20, 30, 40, 60]
            labels = ['Freezing', 'Cold', 'Cool', 'Mild', 'Warm', 'Hot']
            expected = pd.cut(self.data['temperature_celsius'], bins=bins, labels=labels)

            mismatches = self.data[self.data['temperature_category'] != expected]
            for idx, row in mismatches.iterrows():
                expected_cat = expected[idx]
                self.validation_results['validation_flags'].append(
                    f"Record {idx}: temperature_category incorrect. Got '{row['temperature_category']}', expected '{expected_cat}'"
                )
            self.validation_results['flagged_records'] += len(mismatches)

    def _validate_temperature_deviation(self):
        if 'temperature_deviation' in self.data.columns:
            temp_col = None
            if 'temperature_celsius' in self.data.columns:
                temp_col = 'temperature_celsius'
            elif 'temperature_fahrenheit' in self.data.columns:
                temp_col = 'temperature_fahrenheit'

            if temp_col:
                mean_temp = self.data[temp_col].mean()
                expected_deviation = self.data[temp_col] - mean_temp

                # Use a tolerance for float comparisons
                mismatches = self.data[abs(self.data['temperature_deviation'] - expected_deviation) > 0.01]
                for idx, row in mismatches.iterrows():
                    expected_dev = expected_deviation[idx]
                    self.validation_results['validation_flags'].append(
                        f"Record {idx}: temperature_deviation incorrect. Got {row['temperature_deviation']:.2f}, expected {expected_dev:.2f}"
                    )
                self.validation_results['flagged_records'] += len(mismatches)

    def _validate_air_quality_category(self):
        if 'air_quality_us-epa-index' in self.data.columns and 'air_quality_category' in self.data.columns:
            index_to_category = {
                1: 'Good',
                2: 'Moderate',
                3: 'Unhealthy for Sensitive Groups',
                4: 'Unhealthy',
                5: 'Very Unhealthy',
                6: 'Hazardous'
            }
            for idx, row in self.data.iterrows():
                index = row['air_quality_us-epa-index']
                expected = index_to_category.get(index, 'Unknown')
                if row['air_quality_category'] != expected:
                    self.validation_results['validation_flags'].append(
                        f"Record {idx}: air_quality_category incorrect. Got '{row['air_quality_category']}', expected '{expected}'"
                    )
                    self.validation_results['flagged_records'] += 1

    def get_validation_summary(self):
        return {
            'total_records': len(self.data),
            'valid_records': self.validation_results['valid_records'],
            'flagged_records': self.validation_results['flagged_records'],
            'flag_count': len(self.validation_results['validation_flags'])
        }

    def get_validation_results(self):
        return self.validation_results

    def get_flagged_records(self):
        if self.data is None:
            return pd.DataFrame()
        pattern = r"Record (\d+):"
        indices = set()
        for flag in self.validation_results['validation_flags']:
            match = re.search(pattern, flag)
            if match:
                indices.add(int(match.group(1)))
        return self.data.loc[list(indices)]


if __name__ == "__main__":
    processor = Processor(proceed_with_errors=True)
    processor.process()
    processed_data = processor.get_processed_data()

    backup_validator = BackupValidator(processed_data=processed_data)
    backup_validator.validate()

    print("Backup Validation Summary:")
    print(backup_validator.get_validation_summary())

    flags = backup_validator.get_validation_results()['validation_flags']
    if flags:
        print("\nSample validation flags (first 10):")
        for i, flag in enumerate(flags[:10]):
            print(flag)
        if len(flags) > 10:
            print(f"...and {len(flags) - 10} more flags")
