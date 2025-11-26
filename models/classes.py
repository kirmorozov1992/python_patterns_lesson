from models.interfaces import Logger, LoggerFactory

from pathlib import Path
from datetime import datetime
import json
import csv

class Config:

    _instance = None
    _initialized = False
    _log_files = {
        "text": "log.txt",
        "json": "log.json",
        "csv": "log.csv",
    }
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, *, log_dir: str, log_level: str):
        if not self._initialized:
            self._initialized = True
            self.log_dir = log_dir
            self.log_level = log_level  

class TextLogger(Logger):

    def __init__(self, *, config: Config):
        self.log_dir = config.log_dir
        self.log_level = config.log_level
        self.log_file = config._log_files["text"]

    def log(self, *, message: str):
        log_file = Path(self.log_dir) / self.log_file
        log_file.touch(exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_file, "a") as f:
            f.write(f"{timestamp} {self.log_level} {message}\n")

class JSONLogger(Logger):
    def __init__(self, *, config: Config):
        self.log_dir = config.log_dir
        self.log_level = config.log_level
        self.log_file = config._log_files["json"]

    def log(self, *, message: str):
        log_file = Path(self.log_dir) / self.log_file
        log_file.touch(exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_file, "a") as f:
            f.write(
                json.dumps(
                    { "timestamp": timestamp, "level": self.log_level, "message": message }) + "\n"
                )
            
class CSVLogger(Logger):
    def __init__(self, *, config: Config):
        self.log_dir = config.log_dir
        self.log_level = config.log_level
        self.log_file = config._log_files["csv"]

    def log(self, *, message: str):
        log_file = Path(self.log_dir) / self.log_file
        log_file.touch(exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, self.log_level, message])

class TextLoggerFactory(LoggerFactory):
    def __init__(self, config: Config):
        self.config = config

    def create_logger(self):
        return TextLogger(config=self.config)
    
class JSONLoggerFactory(LoggerFactory):
    def __init__(self, config: Config):
        self.config = config

    def create_logger(self):
        return JSONLogger(config=self.config)

class CSVLoggerFactory(LoggerFactory):
    def __init__(self, config: Config):
        self.config = config

    def create_logger(self):
        return CSVLogger(config=self.config)


