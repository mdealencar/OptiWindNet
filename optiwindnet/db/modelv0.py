# SPDX-License-Identifier: MIT

from ._core import _naive_utc_now
from ._peewee import (
    AutoField,
    BlobField,
    DateTimeField,
    FloatField,
    ForeignKeyField,
    IntArrayField,
    IntegerField,
    Model,
    TextField,
    make_db_namespace,
    open_sqlite_database,
)

__all__ = ('define_entities', 'open_database')


def define_entities(database):
    class BaseModel(Model):
        pass

    BaseModel._meta.database = database

    class NodeSet(BaseModel):
        digest = BlobField(primary_key=True)
        name = TextField(unique=True)
        handle = TextField(unique=True)
        T = IntegerField()
        R = IntegerField()
        VertexC = BlobField()
        boundary = BlobField()
        landscape_angle = FloatField(null=True)

        class Meta:
            table_name = 'NodeSet'

    class Method(BaseModel):
        digest = BlobField(primary_key=True)
        funname = TextField()
        funhash = BlobField()
        options = TextField()
        timestamp = DateTimeField(default=_naive_utc_now)

        class Meta:
            table_name = 'Method'

    class Machine(BaseModel):
        id = AutoField()
        name = TextField(unique=True)

        class Meta:
            table_name = 'Machine'

    class EdgeSet(BaseModel):
        id = AutoField()
        nodes = ForeignKeyField(NodeSet, backref='EdgeSets', column_name='nodes')
        edges = BlobField()
        length = FloatField()
        D = IntegerField(default=0, null=True)
        clone2prime = IntArrayField(null=True)
        gates = IntArrayField()
        method = ForeignKeyField(Method, backref='EdgeSets', column_name='method')
        capacity = IntegerField()
        runtime = FloatField(null=True)
        runtime_unit = TextField(null=True)
        machine = ForeignKeyField(Machine, backref='EdgeSets', column_name='machine', null=True)
        timestamp = DateTimeField(null=True, default=_naive_utc_now)
        misc = BlobField(null=True)

        class Meta:
            table_name = 'EdgeSet'

    return make_db_namespace(
        database,
        NodeSet=NodeSet,
        EdgeSet=EdgeSet,
        Method=Method,
        Machine=Machine,
    )


def open_database(filepath: str, create_db: bool = False):
    db = open_sqlite_database(filepath, create_db=create_db)
    model_ns = define_entities(db)
    db.create_tables([model_ns.NodeSet, model_ns.Method, model_ns.Machine, model_ns.EdgeSet])
    return model_ns
