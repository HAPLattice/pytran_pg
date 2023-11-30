import importlib.util as imp_util
from json import dumps as jsondumps
import re
import os
from pathlib import Path
import psycopg2
import pypgoutput
from psycopg2.extras import NamedTupleCursor
import yaml
from typing import Iterable


class BadConfig(Exception):
    pass


class adict(dict):
    def __init__(self, *args, **kwargs):
        args = self._walk_container(args)
        kwargs = self._walk_container(kwargs)
        super().__init__(*args, **kwargs)

    def __getattr__(self, key, default=None):
        if key not in self:
            return default
        return super().__getitem__(key)
    
    def __setattr__(self, key, value):
        return super().__setitem__(key, value)
    
    def copy(self):
        return self.__class__(self)
    
    def _walk_container(self, container):
        if isinstance(container, (list, tuple)):
            if container is None or (len(container) == 0):
                return container

            out = []
            for e in container:
                out.append(self._walk_container(e))

            if isinstance(container, tuple):
                out = tuple(out)

            return out
        elif isinstance(container, dict):
            if container is None or not len(container):
                return container

            out = self.__class__()
            for k, v in container.items():
                out[k] = self._walk_container(v)

            return out
        else:
            return container


class Settings:
    def __init__(
        self,
        config_path: str = os.environ.get("PYTRAN_CONFIG"),
        source_url: str = os.environ.get('PYTRAN_SOURCE_URL'),
        target_url: str = os.environ.get('PYTRAN_TARGET_URL'),
        slot_name: str = os.environ.get('PYTRAN_SOURCE_SLOT'),
        pub_name: str = os.environ.get('PYTRAN_SOURCE_PUBLICATION'),
    ):
        self.source_url = source_url
        self.target_url = target_url
        self.slot_name = slot_name
        self.pub_name = pub_name
        self.config_file_name = Path(os.path.abspath(config_path))
        self._load_config()
        self._parse_config()

    def coalesce(self, *args):
        for a in args:
            if a is not None:
                return a
        return None

    def _load_config(self):
        if self.config_file_name.exists():
            with open(self.config_file_name, 'rt') as config_file:
                self.config = adict(yaml.safe_load(config_file))
        else:
            raise FileExistsError(f"Config file {self.config_file_name} cannot be found.")

    def _parse_config(self):
        if getattr(self, 'config', None) is None:
            raise BadConfig("Config is empty.")
        
        conf = self.config
        self.source_url = self.coalesce(self.source_url, conf.get('source_url'))
        self.target_url = self.coalesce(self.target_url, conf.get('target_url'))
        self.slot_name = self.coalesce(self.slot_name, conf.get('slot_name'))
        self.pub_name = self.coalesce(self.pub_name, conf.get('pub_name'))

        default_transform = conf.default_transform
        for schema_name, schema in conf.schemata.items():
            for table_name, table in schema.items():
                table_xforms = adict(
                    name=(default_transform.name or adict()), 
                    type=(default_transform.type or adict()),
                )
                if table.transform_map is not None:
                    table_xforms.name.update(table.transform_map.name or adict())
                    table_xforms.type.update(table.transform_map.type or adict())

                table.transform_map = Transform(schema_name, table_name, table_xforms, table.transform_file)

                output_xforms = table.output_map or adict()
                table.output_map = MapRecord(schema_name, table_name, output_xforms)


class PGTokenTransform:
    valid_pg_name_regex = re.compile('^[A-z][A-z]*_*[0-9]*$')

    def is_valid_pg_name(self, token):
        return (
            (' ' not in token) and 
            (self.valid_pg_name_regex.match(token) is not None)
        )

    def quote_literal(self, token):
        if isinstance(token, str):
            return f"'{token}'"
        else:
            return token

    def quote_identifier(self, token):
        if self.is_valid_pg_name(token):
            return token
        else:
            return f'"{token}"'


class Transform(PGTokenTransform):
    def __init__(self, schema, table, mapping, transforms_file=None):
        self.schema = schema
        self.table = table
        self.map = adict()
        self._get_column_name_transforms(mapping)
        self._get_column_type_transforms(mapping)
        if transforms_file:
            self._load_transforms(transform_file)
    
    def _get_column_name_transforms(self, mapping):
        name_transforms = mapping.get("name")
        if name_transforms:
            self.map.name = self._get_transforms(name_transforms)
        else:
            self.map.name = adict()
    
    def _get_column_type_transforms(self, mapping):
        type_transforms = mapping.get("type")
        if type_transforms:
            self.map.type = self._get_transforms(type_transforms)
        else:
            self.map.type = adict()
    
    def _get_transforms(self, mapping):
        out = adict()
        for key, xform in mapping.items():
            default_transform = getattr(__builtins__, xform, None)
            xform = getattr(self, xform, default_transform)
            out[key] = xform
        
        return out
    
    def _load_transforms(self, transforms_file):
        mod = self._load_module(transform_file)
        sys.map.name.update(mod.name_transforms)
        sys.map.type.update(mod.type_transforms)
    
    def _load_module(self, path_to_module):
        module_file = Path(path_to_module)
        module_name = module_file.stem

        spec = imp_util.spec_from_file_location(module_name, module_file)
        mod = imp_util.module_from_spec(spec)
        sys.modules[module_name] = mod
        spec.loader.exec_module(mod)

        return mod
    
    def __call__(self, record):
        return self.transform(record)
    
    def __repr__(self):
        return f"Transform(schema={self.schema}, table={self.table}, name_transforms={len(self.map.name)}, type_transforms={len(self.map.type)})"
    
    def __str__(self):
        return repr(self)

    def transform(self, record):
        name_transforms = self.map.name
        type_transforms = self.map.type

        for col in range(record):
            col_name = record[col].name
            col_type = record[col].type
            if col_name in name_transforms:
                record[col].value = name_transforms[col_name](record[col].value)
            elif col_type in type_transforms:
                record[col].value = type_transforms[col_type](record[col].value)
        
        return record

    def stringify_dict(self, token):
        return jsondumps(token, default=str)

    def stringify_iterable(self, token):
        return f"""{','.join(stringify(self.quote_literal(t)) for t in token)}"""

    def str(self, token):
        if isinstance(token, dict):
            return stringify_dict(token)
        elif isinstance(token, list):
            return stringify_iterable(token)
        elif isinstance(token, tuple):
            return stringify_iterable(token)
        else:
            return str(token)

    def extract_range(self, token):
        return (token.lower, token.upper)


class MapRecord:
    def __init__(self, schema, table, recordmap):
        self.schema = schema
        self.table = table
        self.recordmap = recordmap or adict()

    def __call__(self, record):
        return self.apply_map(record)
    
    def __repr__(self):
        return f"MapRecord(schema={self.schema}, table={self.table}, mappings={len(self.recordmap)})"

    def apply_map(self, record):
        if self.recordmap:
            return adict({dcol: record.get(scol) for scol, dcol in self.recordmap.items()})
        else:
            return record


def make_settings(args):
    pass


class Action(PGTokenTransform):
    def __init__(self, action, schema, table, data, **kwargs):
        self.action = action
        self.schema = schema
        self.table = table
        self.data = data
        self._meta = adict(kwargs)
        self.L = self.quote_literal
        self.I = self.quote_identifier
        self.N = self.named_parameter
    
    def statement(self):
        return self.action.lower()
    
    def named_parameter(self, token):
        return f"%({token})s"


class Begin(Action):
    def __init__(self, **kwargs):
        super().__init__('BEGIN', None, None, None, **kwargs)


class Commit(Action):
    def __init__(self, **kwargs):
        super().__init__('COMMIT', None, None, None, **kwargs)


class Rollback(Action):
    def __init__(self, **kwargs):
        super().__init__('ROLLBACK', None, None, None, **kwargs)


class Truncate(Action):
    def __init__(self, **kwargs):
        super().__init__('TRUNCATE', schema, table, None, **kwargs)
        self.stmt_tmpl = f'truncate table {self.I(self.schema)}.{self.I(self.table)}'
    
    def statement(self):
        return stmt_tmpl


class Insert(Action):
    def __init__(self, schema, table, data, **kwargs):
        super().__init__('INSERT', schema, table, data, **kwargs)
        self.stmt_tmpl = """insert into {schema}.{table} ({{tcols}}) values ({{tvals}})""".format(
            schema=self.I(self.schema), table=self.I(self.table)
        )
    
    def statement(self):
        tcols = list(data)
        tvals = ", ".join(self.N(c) for c in tcols)
        tcols = ", ".join(self.I(c) for c in tcols)
        return (self.stmt_tmpl.format(tcols=tcols, tvals=tvals), self.data)


class Update(Action):
    def __init__(self, schema, table, data, **kwargs):
        super().__init__('UPDATE', schema, table, data, **kwargs)
        self.stmt_tmpl = """update {schema}.{table} set {{uspec}} where {{wspec}}""".format(
            schema=self.I(self.schema), table=self.I(self.table)
        )

    def statement(self):
        tcols = [c for c in data if c != self._meta.pk]
        uspec = ", ".join(f"{self.I(c)} = {self.N(c)}" for c in tcols)
        wspec = f"where {self.I(self._meta.pk)} = {self.N(self._meta.pk)}"
        return (self.stmt_tmpl.format(uspec=uspec, wspec=wspec), self.data)


class Delete(Action):
    def __init__(self, schema, table, data, **kwargs):
        super().__init__('DELETE', schema, table, data, **kwargs)
        self.stmt_tmpl = """delete from {schema}.{table} where {{wspec}}""".format(
            schema=self.I(self.schema), table=self.I(self.table)
        )

    def statement(self):
        wspec = f"where {self.I(self._meta.pk)} = {self.N(self._meta.pk)}"
        return (self.stmt_tmpl.format(uspec=uspec, wspec=wspec), {self._meta.pk: self.data[self._meta.pk]})


class BaseDBConnector:
    def __init__(self, dsn: str):
        self.dsn = dsn
    
    def connect(self, autocommit=True):
        self.conn = psycopg2.connect(target_url or self.source_url, cursor_factory=NamedTupleCursor)
        # TODO: autocommit stuffs here!
    
    def execute(self, sql, params=None):
        cur = self.conn.cursor()
        cur.execute(sql, params)
        return cur

    def close(self):
        self.conn.close()
    
    def commit(self):
        self.conn.commit()
    
    def rollback(self):
        self.conn.rollback(self)
    

class ReplicationReader(BaseDBConnector):
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pub_name = self.settings.pub_name
        self.slot_name = self.settings.slot_name
        self.reader_url = self.settings.reader_url
        super().__init__(self.reader_url)
    
    def connect(self):
        self.conn = pypgoutput.LogicalReplicationReader(
            publication=self.pub_name,
            slot_name=self.slot_name,
            dsn=self.reader_url,
        )


class DBWriter(BaseDBConnector):
    def __init__(self, settings: Settings):
        self.settings = settings
        super().__init__(self.settings.writer_dsn)


class FullSyncReader(BaseDBConnector):
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pub_name = self.setting.publication
        super().__init__(self.settings.reader_dsn)

    def get_all_tables(self):
        sql = """
select n.nspname::text as schema_name,
       c.relname::text as table_name,
       array_agg(a.attname::text order by a.attnum) as columns,
       array_agg(coalesce(ta.typname || '[]', t.typname)::text order by a.attnum) as column_types
  from pg_class as c
  join pg_namespace as n
    on n.oid = c.relnamespace
  join pg_attribute as a
    on a.attrelid = c.oid
  join pg_type as t
    on t.oid = a.atttypid
  left
  join pg_type as ta
    on ta.typarray = t.oid
   and t.typcategory = 'A'
 where n.nspname != 'information_schema'
   and n.nspname !~ '^pg_'
   and c.relkind = 'r'
   and a.attnum > 0
 group
    by n.nspname,
       c.relname
 order
    by n.nspname,
       c.relname;
        """
        tables = [rec for rec in self.execute(sql)]
        return tables

    def get_publication(self):
        sql = "select * from pg_publication where pubname = %s ;"
        return self.execute(sql, (self.pub_name,)).fetchone()

    def get_tables(self):
        pub_rec = self.get_publication()
        if pub_rec.puballtables:
            return self.get_all_tables()
        else:
            return self.get_publication_tables()

    def get_publication_tables(self):
        sql = """
select n.nspname::text as schema_name,
       c.relname::text as table_name,
       array_agg(a.attname::text order by a.attnum) as columns,
       array_agg(coalesce(ta.typname || '[]', t.typname)::text order by a.attnum) as column_types
  from pg_get_publication_tables(%s) as p
  join pg_class as c
    on c.oid = p.relid
  join pg_namespace as n
    on n.oid = c.relnamespace
  join pg_attribute as a
    on a.attrelid = c.oid
  join pg_type as t
    on t.oid = a.atttypid
  left
  join pg_type as ta
    on ta.typarray = t.oid
   and t.typcategory = 'A'
 where a.attnum > 0
   and (
           (p.attrs is null) or
           (p.attrs is not null and a.attnum = any(p.attrs))
       )
 group
    by n.nspname,
       c.relname
 order
    by n.nspname,
       c.relname;
"""
        tables = [rec for rec in self.execute(sql, self.pub_name)]
        
        for tab in tables:
            yield tab

    def __iter__(self):
        return self
    
    def next(self):
        for table_rec in self.get_tables():
            for record in self.get_table_records(table_rec.schema_name, table_rec.table_name, table_rec.columns):
                # TODO: Determine if connection.copy() can be used with redshift
                yield Insert(
                    table_rec.schema_name, 
                    table_rec.table_name, 
                    record, 
                    adict(
                        message_id=uuid.uuid4(), 
                        message_time=datetime.now(tz=timezone.utc), 
                        sync_action="full"
                    )
                )

    def get_table_records(self, schema_name: str, table_name:str, columns: Iterable[str]):
        for record in self.execute(f"""select * from "{self.schema}"."{self.table}" ;"""):
            yield record
    

