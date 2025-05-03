from reader import Reader
from validator import Validator 
from processor import Processor 
from backupvalidator import BackupValidator 
from writer import Writer 

def main():
    
    
    # Reader step
    reader = Reader.read_last_file()  
    
    # Validation step
    validator = Validator(reader)
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
    writer = Writer("DefaultEndpointsProtocol=https;AccountName=uiiauiiau;AccountKey=ZxKBlPoSrGjlXyHwFUQLe1l7Ps74FVGs4j27S2QBCeOtYnGO+be0020Krs37xlOFMaXiGQN23s4++ASt+O0Tpg==;EndpointSuffix=core.windows.net", "weather")
    writer.write(processed_data, "processed_weather.csv", 'Weather Real-Time Processing/output/processed_weather.csv')
    
if __name__ == "__main__":
    main()