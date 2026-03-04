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
    JsonCompatField,
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
        name = TextField(unique=True)
        T = IntegerField()
        R = IntegerField()
        VertexC = BlobField()
        boundary = BlobField()
        landscape_angle = FloatField(null=True)
        digest = BlobField(primary_key=True)

        class Meta:
            table_name = 'NodeSet'

    class Method(BaseModel):
        funname = TextField()
        options = JsonCompatField()
        timestamp = DateTimeField(default=_naive_utc_now)
        funfile = TextField()
        funhash = BlobField()
        digest = BlobField(primary_key=True)

        class Meta:
            table_name = 'Method'

    class Machine(BaseModel):
        id = AutoField()
        name = TextField(unique=True)
        attrs = JsonCompatField(null=True)

        class Meta:
            table_name = 'Machine'

    class EdgeSet(BaseModel):
        id = AutoField()
        handle = TextField()
        capacity = IntegerField()
        length = FloatField()
        runtime = FloatField(null=True)
        machine = ForeignKeyField(Machine, backref='EdgeSets', column_name='machine', null=True)
        gates = IntArrayField()
        T = IntegerField()
        R = IntegerField()
        D = IntegerField(default=0, null=True)
        timestamp = DateTimeField(null=True, default=_naive_utc_now)
        misc = JsonCompatField(null=True)
        clone2prime = IntArrayField(null=True)
        edges = IntArrayField()
        nodes = ForeignKeyField(NodeSet, backref='EdgeSets', column_name='nodes')
        method = ForeignKeyField(Method, backref='EdgeSets', column_name='method')

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
