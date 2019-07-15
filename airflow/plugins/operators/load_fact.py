from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    # load_fact_sql = """
    # INSERT INTO public.songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    # SELECT
    #     TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time,
    #     e.userid,
    #     e.level,
    #     s.song_id,
    #     s.artist_id,
    #     e.sessionid,
    #     e.location,
    #     e.useragent
    # FROM public.staging_events e
    # LEFT JOIN public.staging_songs s
    # ON e.song = s.title
    # AND e.artist = s.artist_name
    # WHERE e.page = 'NextSong';
    # """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 # create_table_sql="",
                 load_fact_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        # self.create_table_sql=create_table_sql
        self.load_fact_sql=load_fact_sql

    def execute(self, context):
        self.log.info('Loading Fact Table.')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # redshift.run(self.create_table_sql)
        redshift.run(self.load_fact_sql)
