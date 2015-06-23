__author__ = 'Nick Demyanchuk'
__email__ = 'spike@scalr.com'

import json

import peewee
from playhouse import db_url

DB_PROXY = peewee.Proxy()


def connect_to_db(url):
    database = db_url.connect(url)
    database.register_fields({'json': 'json'})
    DB_PROXY.initialize(database)
    for model in SCALR_ENTITIES:
        model.create_table(fail_silently=True)


class JsonField(peewee.CharField):
    def db_value(self, value):
        return json.dumps(value)

    def python_value(self, value):
        try:
            return json.loads(value)
        except:
            return value


def db_table_name_for_model(model):
    return "{}s".format(model.__class__.__name__.lower())


class HabibiModel(peewee.Model):

    class Meta:
        database = DB_PROXY
        db_table_func = db_table_name_for_model


class Farm(HabibiModel):
    name = peewee.CharField(unique=True)
    status = peewee.CharField(default='terminated')


class Role(HabibiModel):
    name = peewee.CharField(unique=True)
    image = peewee.CharField()
    behaviors = peewee.CharField()


class FarmRole(HabibiModel):
    farm = peewee.ForeignKeyField(Farm, related_name='farm_roles')
    role = peewee.ForeignKeyField(Role)
    orchestration = JsonField()


class Server(HabibiModel):
    id = peewee.CharField(primary_key=True)
    index = peewee.IntegerField()
    zone = peewee.CharField()
    farm_role = peewee.ForeignKeyField(FarmRole, related_name='servers')
    public_ip = peewee.CharField(null=True)
    private_ip = peewee.CharField(null=True)
    host_machine = peewee.CharField(null=True)
    container_id = peewee.CharField(null=True)
    volumes = peewee.TextField(null=True)
    status = peewee.CharField(default='pending launch')


class Event(HabibiModel):
    name = peewee.CharField()
    event_id = peewee.CharField()
    triggering_server = peewee.ForeignKeyField(Server, related_name='sent_events')


class GlobalVariable(HabibiModel):
    scopes_available = ('farm', 'role', 'farm_role', 'server')

    name = peewee.CharField()
    scopes = peewee.CharField()

SCALR_ENTITIES = (Farm, Role, FarmRole, Server, Event, GlobalVariable)
