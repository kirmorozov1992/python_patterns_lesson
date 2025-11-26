from abc import ABC, abstractmethod

class Logger(ABC):

    @abstractmethod
    def log():
        pass

class LoggerFactory(ABC):

    @abstractmethod
    def create_logger(self) -> Logger:
        pass

