# -*- coding: utf-8 -*-
import sys
import json
import logging

import six
import peewee
from playhouse import db_url

import habibi.exc


DB_PROXY = peewee.Proxy()
LOG = logging.getLogger(__name__)
logger = logging.getLogger('peewee')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


def connect_to_db(url):
    """Connect to DB specified in url,
       create tables for all habibi models.

    :type  url: str
    :param url: Database url connection string.
    :return: database` object
    :return type: peewee.Database
    """
    database = db_url.connect(url)
    database.register_fields({'json': 'json'})

    DB_PROXY.initialize(database)
    for model in SCALR_ENTITIES:
        model.create_table(fail_silently=True)
    return database


def get_model_from_scope(scope):
    """Finds peewee model by scope name.

        :type scope: string
        :rtype: peewee.Model

        :raises habibi.exc.HabibiModelNotFound: if model could not be found

        Examples:
            get_model_from_scope('farm')      # Returns Farm class
            get_model_from_scope('farm_role') # Returns FarmRole class

    """
    model_name = "".join([word.capitalize() for word in scope.split('_')])
    try:
        model = getattr(sys.modules[__name__], model_name)
        return model
    except (AttributeError, AssertionError) as e:
        six.raise_from(habibi.exc.HabibiModelNotFound(model_name), e)


class JsonField(peewee.TextField):
    """Custom peewee field that stores JSON-like object in text field."""
    def db_value(self, value):
        return json.dumps(value)

    def python_value(self, value):
        try:
            return json.loads(value)
        except:
            return value


def db_table_name_for_model(model):
    return "habibi_{}s".format(model.__name__.lower())


class HabibiModel(peewee.Model):
    """Class that definesq DB backend, and table naming convention.
       All habibi models should inherit from this class.
    """
    class Meta(object):
        database = DB_PROXY
        db_table_func = db_table_name_for_model


class Farm(HabibiModel):
    name = peewee.CharField(unique=True, index=True)


class Role(HabibiModel):
    name = peewee.CharField(unique=True, index=True)
    image = peewee.CharField()
    behaviors = JsonField()


class FarmRole(HabibiModel):
    farm = peewee.ForeignKeyField(Farm, related_name='farm_roles', on_delete='CASCADE')
    role = peewee.ForeignKeyField(Role)
    orchestration = JsonField()


class Server(HabibiModel):
    id = peewee.CharField(primary_key=True)
    index = peewee.IntegerField()
    farm_role = peewee.ForeignKeyField(FarmRole, related_name='servers')
    public_ip = peewee.CharField(null=True)
    private_ip = peewee.CharField(null=True)
    host_machine = peewee.CharField(null=True)
    container_id = peewee.CharField(null=True)
    volumes = JsonField()
    status = peewee.CharField(default='pending launch')


class Event(HabibiModel):
    name = peewee.CharField()
    id = peewee.CharField(primary_key=True)
    triggering_server = peewee.ForeignKeyField(Server, related_name='sent_events')


class GlobalVariable(HabibiModel):
    name = peewee.CharField(primary_key=True)
    scopes = JsonField()


SCALR_ENTITIES = (Farm, Role, FarmRole, Server, Event, GlobalVariable)
