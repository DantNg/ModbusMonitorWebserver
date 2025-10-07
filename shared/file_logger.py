"""
File Logger - Handles CSV and JSON file logging
"""

import csv
import json
import os
from datetime import datetime
from typing import List, Dict, Any

class FileLogger:
    def __init__(self, output_dir: str = "./logs"):
        self.output_dir = output_dir
        self.ensure_directory()
    
    def ensure_directory(self):
        """Ensure output directory exists"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def write_csv(self, filename: str, data: List[Dict[str, Any]], append: bool = True):
        """Write data to CSV file"""
        try:
            mode = 'a' if append else 'w'
            file_exists = os.path.exists(filename)
            
            if not data:
                return
            
            with open(filename, mode, newline='', encoding='utf-8') as csvfile:
                fieldnames = data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header only if file is new or in write mode
                if not file_exists or not append:
                    writer.writeheader()
                
                writer.writerows(data)
                
        except Exception as e:
            print(f"❌ CSV write error: {e}")
    
    def append_json(self, filename: str, data: Dict[str, Any]):
        """Append data to JSON lines file (JSONL format)"""
        try:
            with open(filename, 'a', encoding='utf-8') as jsonfile:
                json.dump(data, jsonfile, default=str, ensure_ascii=False)
                jsonfile.write('\n')
                
        except Exception as e:
            print(f"❌ JSON append error: {e}")
    
    def write_json(self, filename: str, data: Any):
        """Write data to JSON file (overwrites)"""
        try:
            with open(filename, 'w', encoding='utf-8') as jsonfile:
                json.dump(data, jsonfile, indent=2, default=str, ensure_ascii=False)
                
        except Exception as e:
            print(f"❌ JSON write error: {e}")
    
    def read_csv(self, filename: str) -> List[Dict[str, Any]]:
        """Read data from CSV file"""
        try:
            data = []
            with open(filename, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    data.append(dict(row))
            return data
            
        except Exception as e:
            print(f"❌ CSV read error: {e}")
            return []
    
    def read_json_lines(self, filename: str) -> List[Dict[str, Any]]:
        """Read data from JSON lines file"""
        try:
            data = []
            with open(filename, 'r', encoding='utf-8') as jsonfile:
                for line in jsonfile:
                    if line.strip():
                        data.append(json.loads(line))
            return data
            
        except Exception as e:
            print(f"❌ JSON lines read error: {e}")
            return []
    
    def cleanup_old_files(self, max_files: int = 100):
        """Clean up old log files, keeping only the most recent ones"""
        try:
            files = []
            for filename in os.listdir(self.output_dir):
                if filename.endswith(('.csv', '.json')):
                    filepath = os.path.join(self.output_dir, filename)
                    files.append((filepath, os.path.getmtime(filepath)))
            
            # Sort by modification time (newest first)
            files.sort(key=lambda x: x[1], reverse=True)
            
            # Delete old files
            for filepath, _ in files[max_files:]:
                try:
                    os.remove(filepath)
                    print(f"🗑️ Deleted old log file: {os.path.basename(filepath)}")
                except Exception as e:
                    print(f"⚠️ Could not delete {filepath}: {e}")
                    
        except Exception as e:
            print(f"❌ Cleanup error: {e}")