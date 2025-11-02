from abc import ABC, abstractmethod
from typing import Any

class MetadataProcessor(ABC):
    @abstractmethod
    def get_datetime_job_run(self, *, conn: Any):
        pass

    @abstractmethod
    def update_datetime_job_run(self, *, conn: Any, **kwargs):
        pass

    @abstractmethod
    def get_current_date(self, *, conn: Any):
        pass

    @abstractmethod
    def get_column_names(self, *, conn: Any, **kwargs):
        pass

    @abstractmethod
    def get_clauses(self, *, conn: Any, **kwargs):
        pass

    @abstractmethod
    def get_job_meta(self, *, conn: Any, **kwargs):
        pass

    @abstractmethod
    def get_id_col_data_type(self, *, conn: Any, **kwargs):
        pass

    @abstractmethod
    def get_old_log_count(self, *, conn: Any, **kwargs):
        pass

    @abstractmethod
    def get_new_log_count(self, *, conn: Any, **kwargs):
        pass

    @abstractmethod
    def set_log_count(self, *, conn: Any, **kwargs):
        pass

    @abstractmethod
    def make_day_offset(self, *, conn: Any, **kwargs):
        pass

    @abstractmethod
    def get_min_max_value(self, *, conn: Any, **kwargs):
        pass
    

class Creator(ABC):
    @abstractmethod
    def create_table(self, *, conn: Any, **kwargs):
        pass

class Snapshotter(ABC):
    @abstractmethod
    def snapshot(self, *, conn: Any):
        pass

class Loader(ABC):
    @abstractmethod
    def collect_data(self, *, conn: Any, **kwargs):
        pass

    @abstractmethod
    def insert(self, *, conn: Any, **kwargs):
        pass

    @abstractmethod
    def upsert(self, *, conn: Any, **kwargs):
        pass

    @abstractmethod
    def process_data(self, *, conn: Any, **kwargs):
        pass

