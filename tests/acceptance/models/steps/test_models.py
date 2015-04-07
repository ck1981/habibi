__author__ = 'spike'

import behave
from mongoengine import connection, connect

import habibi


@behave.given('I created farm with 1 role')
def i_created_farm(context):
    context.farm = habibi.Habibi('spike-test')
    context.farm.save()
    context.role = context.farm.add_role('myrole', ['app', 'www'])
    context.farm.start()

@behave.when('I add 2 servers')
def i_add_servers(context):
    context.servers = []
    for _ in range(2):
        server = context.role.add_server()
        context.servers.append(server)

@behave.then('I can see that farm was updated')
def farm_was_saved(context):
    context.farm.reload()
    assert len(context.farm.roles) == 1
    role = context.farm.roles[0]
    assert len(role.servers) == 2

@behave.when('I reconnect')
def reconnect(context):
    conn = connection.get_connection()
    conn.disconnect()

    connect(context.db_name)

@behave.then('I see that farm i added before, with same servers')
def farm_is_ok(context):
    farm = habibi.Habibi.objects[0]
    for attrib in ('id', 'name', 'farm_crypto_key', 'status'):
        assert  getattr(farm, attrib) == getattr(context.farm, attrib)

    assert len(farm.roles) == 1

    role = farm.roles[0]
    assert len(role.servers) == 2
    assert sorted([x.id for x in role.servers]) == sorted([x.id for x in context.servers])