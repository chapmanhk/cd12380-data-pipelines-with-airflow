from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        self.log.info(f'Starting to copy data from S3 to Redshift table {self.table}')

        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        rendered_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_key}'

        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}' 
            SECRET_ACCESS_KEY '{credentials.secret_key}' 
            FORMAT AS JSON '{self.json_path}' 
            TIMEFORMAT AS 'epochmillisecs' 
            REGION 'us-west-2';
        """

        self.log.info(f'Running COPY command: {copy_sql}')
        redshift_hook.run(copy_sql)
        self.log.info(f'Successfully staged data from {s3_path} to {self.table}')





