__author__ = 'spike'

import behave

from habibi.api import HabibiApi


@behave.given('I created farm with 1 role')
def i_created_farm(ctx):
    api = habibi.api.HabibiApi()
    farm = api.create_farm('spike-test')
    role = api.create_role('ubuntu1204-app', ['app', 'www'])
    farm_role = api.farm_add_role(farm.id, role.id)

    for item, value in locals():
        setattr(ctx, item, value)

@behave.when('I add 2 servers')
def i_add_servers(ctx):
    ctx.servers = [ctx.api.create_server(ctx.farm_role['id'], zone='local_zone_1')
                   for _ in range(2)]

@behave.then('I can see that farm was updated')
def farm_was_saved(ctx):
    farm = ctx.api.get_farm(ctx.farm['id'])
    farm_roles = ctx.api.find_farm_roles()
    assert ctx.farm_role['id'] in farm['farmroles']

    role = ctx.farm.roles[0]
    assert len(role.servers) == 2

@behave.when('I reconnect')
def reconnect(ctx):
    conn = connection.get_connection()
    conn.disconnect()

    connect(ctx.db_name)

@behave.then('I see that farm i added before, with same servers')
def farm_is_ok(ctx):
    farm = habibi.Habibi.objects[0]
    for attrib in ('id', 'name', 'farm_crypto_key', 'status'):
        assert  getattr(farm, attrib) == getattr(ctx.farm, attrib)

    assert len(farm.roles) == 1

    role = farm.roles[0]
    assert len(role.servers) == 2
    assert sorted([x.id for x in role.servers]) == sorted([x.id for x in ctx.servers])