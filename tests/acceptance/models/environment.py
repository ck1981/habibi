import random

from mongoengine import connection
import mongoengine as me


def before_all(context):
    context.db_name = db_name = 'test%d' % random.randint(1, 1000000)
    context.connection = me.connect(db_name)

def after_all(context):
    conn = connection.get_connection()
    conn.drop_database(context.db_name)
