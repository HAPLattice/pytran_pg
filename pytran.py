import redshift_connector as redshift
import yaml
from types import adict
from types import Settings
from types import Transform
from types import MapRecord
from types import make_settings


DEFAULT_CONFIG_FILE_NAME="pytran_config.yanml"


class PyTran:
    def __init__(
        self,
        config_path: str = os.environ.get('PYTRAN_CONFIG')
    ):
        self.settings = Settings(config_path=config_path)
    
    # Always PostgreSQL
    def connect_source(self, target_dsn=None):
        reader = psycopg2.connect(target_dsn or self.source_dsn, cursor_factory=NamedTupleCursor)
        return reader

    def connect_source_replication(self):
        reader = pypgoutput.LogicalReplicationReader(
            publication=self.settings.pub_name,
            slot_name=self.settings.slot_name,
            dsn=self.settings.source_dsn,
        )
        return reader

    # Assume Redshift, but check for PostgreSQL
    def connect_target(self):
        if 'postgresql://' in self.settings.target_dsn:
            conn = self.connect_source(self.settings.target_dsn)
        else:
            conn = redshift.connect(self.settings.target_dsn)
        
        return conn
    
    def run(full_sync=False):
        m_name = "full_sync" if full_sync else "incremental_sync"
        processor = getattr(self, m_name)
        r_conn = self.connect_source()
        try:
            w_conn = self.connect_target()
            try:
                processor(r_conn, w_conn)
            finally:
                w_conn.close()
        finally:
            r_conn.close

    def full_sync(self, read_conn, write_conn):
        for table in self.get_publication_tables(r_conn):
            writer = self.table_writer(w_conn, table)
            for rec in self.table_reader(r_conn, table):
                writer.write(self.process_record())
        
    def incremental_sync(self):
        r_conn = self.connect_source()
        w_conn = self.connect_target()
        try:
            for message in self.source_conn:
                self.process_message(message)
        finally:
            self.source_conn.stop()

    def process_message(self, message):
        # read
        print(message.json(indent=2))
    
    def process_record(self, record):
        pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--source-dsn', type=str, dest='source_dsn', required=True, metavar="S_DSN", help="Source database connection string in URI format")
    parser.add_argument('--target-dsn', type=str, dest='target_dsn', required=True, metavar="T_DSN", help="Target database connection string in URI format")
    parser.add_argument('--slot-name', type=str, dest='slot_name', required=True, metavar='SLOT', help='Source DB replication slot name.')
    parser.add_argument('--pub-name', type=str, dest='pub_name', required=True, metavar='PUB', help='Source DB replication publication name.')
    parser.add_argument('--config', type=str, dest='config_path', required=False, default=DEFAULT_CONFIG_FILE_NAME, metavar="FILE", help="/path/to/config_file.yaml")

    args = parser.parse_args()
    app = PyTran(**adict(args._get_kwargs()))
    app.run()

