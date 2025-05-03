from reader import Reader 
from validator import Validator 
from processor import Processor 
from backupvalidator import BackupValidator 
from writer import Writer 
import os

def main():
    
    
    # Reader step
    reader = Reader()
    data = reader.load_data()
    data.head(2)
    
    
    # Validation step
    validator = Validator(data)
    validator.validate_dataset()
    print("Validation Summary:")
    print(validator.get_validation_summary())
    errors = validator.get_validation_results()['validation_errors']
    if errors:
        print("\nSample validation errors (first 10):")
        for i, error in enumerate(errors[:10]):
            print(error)
        if len(errors) > 10:
            print(f"...and {len(errors) - 10} more errors")
    validator.get_validated_data()
    
    
    # Processor step
    processor = Processor()
    processor.process()
    processed_data = processor.get_processed_data()
    print(processed_data.info())
    
    
    # Back-up Validator step
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
           
           
    #Writer step 
    writer = Writer("DefaultEndpointsProtocol=https;AccountName=uiiauiiau;AccountKey=ZxKBlPoSrGjlXyHwFUQLe1l7Ps74FVGs4j27S2QBCeOtYnGO+be0020Krs37xlOFMaXiGQN23s4++ASt+O0Tpg==;EndpointSuffix=core.windows.net", "nashville")
    # Use the absolute path inside the Docker container for output file
    output_path = os.path.join('/opt/airflow/dags', 'output', 'processed_nashville_housing.csv')
    writer.write(processed_data, "processed_nashville_housing.csv", output_path)
    
if __name__ == "__main__":
    main()