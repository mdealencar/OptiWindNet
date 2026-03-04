# SPDX-License-Identifier: MIT

import datetime

from ._core import _naive_utc_now
from ._peewee import (
    AutoField,
    BlobField,
    BooleanField,
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
        B = IntegerField()
        VertexC = BlobField()
        constraint_groups = IntArrayField()
        constraint_vertices = IntArrayField()
        landscape_angle = FloatField(null=True)
        digest = BlobField(primary_key=True)

        class Meta:
            table_name = 'NodeSet'

    class Method(BaseModel):
        solver_name = TextField()
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

    class RouteSet(BaseModel):
        id = AutoField()
        handle = TextField()
        valid = BooleanField(null=True)
        T = IntegerField()
        R = IntegerField()
        capacity = IntegerField()
        length = FloatField()
        is_normalized = BooleanField()
        runtime = FloatField(null=True)
        num_gates = IntArrayField()
        C = IntegerField(default=0, null=True)
        D = IntegerField(default=0, null=True)
        creator = TextField(null=True)
        detextra = FloatField(null=True)
        num_diagonals = IntegerField(null=True)
        tentative = IntArrayField(null=True)
        rogue = IntArrayField(null=True)
        timestamp = DateTimeField(null=True, default=_naive_utc_now)
        misc = JsonCompatField(null=True)
        stuntC = BlobField(null=True)
        clone2prime = IntArrayField(null=True)
        edges = IntArrayField()
        nodes = ForeignKeyField(NodeSet, backref='RouteSets', column_name='nodes')
        method = ForeignKeyField(Method, backref='RouteSets', column_name='method')
        machine = ForeignKeyField(
            Machine, backref='RouteSets', column_name='machine', null=True
        )

        class Meta:
            table_name = 'RouteSet'

    return make_db_namespace(
        database,
        NodeSet=NodeSet,
        RouteSet=RouteSet,
        Method=Method,
        Machine=Machine,
    )


def open_database(filepath: str, create_db: bool = False):
    db = open_sqlite_database(filepath, create_db=create_db)
    model_ns = define_entities(db)
    db.create_tables([model_ns.NodeSet, model_ns.Method, model_ns.Machine, model_ns.RouteSet])
    return model_ns
