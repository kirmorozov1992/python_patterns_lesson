from models.classes import TextLoggerFactory, JSONLoggerFactory, CSVLoggerFactory, Config
from models.interfaces import Logger

def get_logger(*, type: str, config: Config) -> Logger:
    match type:
        case "text":
            textloggerfactory = TextLoggerFactory(config)
            return textloggerfactory.create_logger()
        case "json":
            jsonloggerfactory = JSONLoggerFactory(config)
            return jsonloggerfactory.create_logger()
        case "csv":
            csvloggerfactory = CSVLoggerFactory(config)
            return csvloggerfactory.create_logger()
        case _:
            print("Unknown logger type")
