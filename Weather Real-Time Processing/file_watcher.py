import os
import time
import logging
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from reader import Reader

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class FileWatcher(FileSystemEventHandler):
    
    def __init__(self):
        self.detected_files = set()
    
    def on_created(self, event):        
        file_path = event.src_path
            
        # Add to detected files set
        self.detected_files.add(file_path)
        
        logger.info(f"  > {file_path}")
        time.sleep(2)
        
def start_monitoring(input_dir):  
    event_handler = FileWatcher()
    observer = Observer()
    observer.schedule(event_handler, input_dir, recursive=False)
    observer.start()
    
    try:
        # Process existing files
        files_found = False
        logger.info("Checking for existing files in the directory...")
        
        for filename in os.listdir(input_dir):
            file_path = os.path.join(input_dir, filename)
            if os.path.isfile(file_path):
                files_found = True
                logger.info(f"  > {filename}")
                # Add to detected files
                event_handler.detected_files.add(file_path)
        
        if not files_found:
            logger.info("No existing files found in the directory.")
        
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")
        observer.stop()
    
    observer.join()

if __name__ == "__main__":
    # Get the absolute path to the input directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(script_dir, "input")
    start_monitoring(input_dir)