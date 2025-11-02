from interfaces import *
from psycopg2.extensions import connection
from datetime import datetime, timedelta

class PostgresMetadataProcessor(MetadataProcessor):
    def __init__(self, *, conn: connection) -> None:
        self.conn = conn

    def get_datetime_job_run(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute("select to_char(now(), 'YYYYMMDDHH24MISS')::bigint;")
            rows = cur.fetchall()
            return rows[0][0]
        
    def update_datetime_job_run(
        self, *,
        job_id: int,
        job_schema: str,
        job_table: str,
        job_run: int     
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            update {job_schema}.{job_table}
                            set job_last_date_n = {job_run}
                            where job_id = {job_id};
                        """)
        self.conn.commit()

    def get_current_date(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute("select to_char(now(), 'YYYYMMDD')::bigint;")
            rows = cur.fetchall()
            return rows[0][0]

    def get_column_names(
        self, *, 
        source_schema: str, 
        source_table: str
    ) -> list:
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            select column_name
                            from information_schema.columns
                            where table_schema = '{source_schema}' 
                            and table_name = '{source_table}'
                            order by ordinal_position;
                        """)
            rows = cur.fetchall()
        columns_list = list()
        for r in rows:
            columns_list.append(r[0])

        return columns_list
    
    def get_clauses(self, *, columns: list) -> tuple[str, str]:
        for c in columns:
            clause_1 = clause_1 + c + ' = excluded.' + c + ','
        clause_1 = clause_1 + 'updated = localtimestamp, is_deleted = false'

        for c in columns:
            clause_2 = clause_2 + 'trg.' + c + ' is distinct from excluded.' + c + ' or '
        clause_2 = clause_2[:-3]

        return clause_1, clause_2
    
    def get_job_meta(
        self, *,
        job_id: int,
        job_schema: str,
        job_table: str
    ) -> tuple[int, int, bool, int]:
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            select job_table_id, job_last_date_n, job_is_disabled, job_last_id
                            from {job_schema}.{job_table}
                            where job_id = {job_id};
                        """)
            rows = cur.fetchall()
            return rows[0]
        
    def get_id_col_data_type(
        self, *,
        table_schema: str,
        table_name: str,
        id_cols: list
    ) -> list:
        id_cols_str = ','.join(f"'{col}'" for col in id_cols)
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            select column_name, data_type
                            from information_schema.columns
                            where table_schema = '{table_schema}'
                            and table_name = '{table_name}'
                            and column_name in ({id_cols_str})
                            order by ordinal_position;
                        """)
            result = cur.fetchall()
        return result
    
    def get_old_log_count(
        self, *,
        lc_schema: str,
        lc_table: str, 
        job_id: int, 
        table_id: int, 
        part_week: int, 
        date_hour: int
    ) -> tuple[int, int]:
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            select cnt_to_upsert, cnt_to_delete
                            from {lc_schema}.{lc_table}
                            where job_id = {job_id}
                            and table_id = {table_id}
                            and part_week = {part_week}
                            and date_hour = {date_hour};
                        """)
            result = cur.fetchone()
        if result is None:
            return 0, 0
        cnt_upsert, cnt_delete = result
        return cnt_upsert, cnt_delete
    
    def get_new_log_count(
        self, *,
        log_schema: str, 
        log_table: str, 
        table_id: int, 
        part_week: int, 
        date_hour: int
    ) -> tuple[int, int]:
        date_hour_start = int(str(date_hour) + '0000')
        if '23' in str(date_hour):
            date_hour_end = int(((datetime.strptime(str(date_hour), "%Y%m%d%H")) + timedelta(hours=1)).strftime("%Y%m%d%H")  + '0000')
        else:
            date_hour_end = int(str(date_hour + 1) + '0000')
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            select
                                count(log_id) filter(where operid < 3),
                                count(log_id) filter(where operid = 3)
                            from {log_schema}.{log_table}
                            where table_id = {table_id}
                            and part_week = {part_week}
                            and logged_at_n between {date_hour_start} and {date_hour_end};
                        """)
            result = cur.fetchone()
        cnt_upsert, cnt_delete = result
        return cnt_upsert, cnt_delete

    def set_log_count(
        self, *,
        lc_schema: str,
        lc_table: str,
        col_name: str,
        new_log_count: int,
        job_id: int,
        table_id: int,
        part_week: int,
        date_hour: int
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            update {lc_schema}.{lc_table}
                            set {col_name} = {new_log_count}
                            where job_id = {job_id}
                            and table_id = {table_id}
                            and part_week = {part_week}
                            and date_hour = {date_hour};
                        """)
        self.conn.commit()

    def make_day_offset(
        self, *,
        lc_schema: str, 
        lc_table: str, 
        table_id: int, 
        job_id: int
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(f"""
                            select date_hour
                            from {lc_schema}.{lc_table}
                            where job_id = {job_id}
                            and table_id = {table_id}
                            and part_week = extract(isodow from current_date - interval '2 day')
                            order by date_hour;
                        """)
            rows = cur.fetchall()
            hours_list = list()
            for r in rows:
                hours_list.append(r[0])

            for i in range(0, len(hours_list)):
                cur.execute(f"""
                                update {lc_schema}.{lc_table}
                                set date_hour = (to_char(current_date, 'YYYYMMDD') || '00')::int + {i},
                                    part_week = extract(isodow from current_date),
                                    cnt_to_upsert = 0,
                                    cnt_to_delete = 0
                                where date_hour = {hours_list[i]}
                                and table_id = {table_id}
                                and job_id = {job_id};
                            """)
        self.conn.commit()

    def get_min_max_value(
        self, *,
        source_schema: str,
        source_table: str, 
        id_cols_str: str
    ) -> tuple[int, int]:
        with self.conn.cursor() as cur:
            cur.execute(f"select min({id_cols_str}) from {source_schema}.{source_table};")
            result = cur.fetchone()
            min_id = result[0]
            cur.execute(f"select max({id_cols_str}) from {source_schema}.{source_table};")
            result = cur.fetchone()
            max_id = result[0]

            return min_id, max_id
        
class PostgresFullSnapshotter(Snapshotter):
    def __init__(self, *, conn: connection) -> None:
        self.conn = conn

    def snapshot(
        self, *,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str
    ):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                truncate table {target_schema}.{target_table};
            """)

            cur.execute(f"""
                insert into {target_schema}.{target_table}
                select * from {source_schema}.{source_table};
            """)

        self.conn.commit()

class PostgresIncrementalSnapshotter(Snapshotter):
    def __init__(self, *, conn: connection):
        self.conn = conn

    def snapshot(
        self, *,
        source_schema: str,
        source_table: str,
        id_col: str,
        target_schema: str,
        target_table: str,
        min_id: int,
        max_id: int,
        offset: int
    ):
        with self.conn.cursor() as cur:
            cur.execute(f"truncate {target_schema}.{target_table};")
            while min_id <= max_id:
                cur.execute(f"""
                    insert into {target_schema}.{target_table}
                    select *, localtimestamp, false
                    from {source_schema}.{source_table}
                    where {id_col} >= {min_id} and {id_col} < {min_id + offset};
                """)
                min_id += offset
        self.conn.commit()
        