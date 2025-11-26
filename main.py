from models.classes import Config
from utils import get_logger

def main():

    try:
        config = Config(log_dir="/home/kirillmorozov/testlogs", log_level="INFO")

        textlogger = get_logger(type="text", config=config)
        jsonlogger = get_logger(type="json", config=config)
        csvlogger = get_logger(type="csv", config=config)

        textlogger.log(message="connect first user")
        jsonlogger.log(message="connect first user")
        csvlogger.log(message="connect first user")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

