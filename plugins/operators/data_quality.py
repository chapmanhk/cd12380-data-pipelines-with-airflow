from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Running {len(self.tests)} data quality checks...")

        for i, test in enumerate(self.tests, start=1):
            sql = test.get('sql')
            
            self.log.info(f" Running test {i}: {sql}")
            result = redshift.get_first(sql)

            if result is None or len(result) == 0:
                raise ValueError(f"Test {i} failed: no result returned for query: {sql}")
            
            value = result[0]
            if value <= 0:
                raise ValueError(f"Test {i} failed: {sql} returned {value}, expected > 0")
            
            self.log.info(f"Test {i} passed: {sql} returned {value}")