__author__ = 'spike'

import json
import random

import behave

import habibi.api as habibi_api


@behave.given('I created habibi api object')
def i_created_api(ctx):
    ctx.api = habibi_api.HabibiApi(base_dir=ctx.base_dir, db_url="sqlite:///:memory:")

@behave.when("I created new farm named '{farm_name}'")
def add_farm(ctx, farm_name):
    ctx.farm = new_farm = ctx.api.create_farm(name=farm_name)

@behave.when("I created new role named '{role_name}'")
def add_role(ctx, role_name):
    role_kwargs = json.loads(ctx.text.strip())
    ctx.role = new_role = ctx.api.create_role(name=role_name, **role_kwargs)

@behave.when('I added this role to my farm')
def create_farmrole(ctx):
    ctx.farm_role = ctx.api.farm_add_role(ctx.farm['id'], ctx.role['id'])

@behave.when("I created {how_much} servers of that new farm_role in zone '{zone}'")
def create_servers(ctx, how_much, zone):
    ctx.servers = list()
    how_much = int(how_much)
    for _ in range(how_much):
        server = ctx.api.create_server(ctx.farm_role['id'], zone=zone)
        ctx.servers.append(server)

@behave.when("I created new event '{ev_name}' triggered by one of my servers")
def new_event(ctx, ev_name):
    ctx.triggering_server = random.choice(ctx.servers)
    ctx.event = ctx.api.create_event(name=ev_name, triggering_server_id=ctx.triggering_server['id'])

@behave.when('I set GVs')
def set_gv(ctx):
    ctx.gvs = []
    for row in ctx.table:
        kwds = dict(zip(row.headings, row.cells))
        print(kwds)
        gv = ctx.api.set_global_variable(**kwds)
        ctx.gvs.append(gv)

@behave.when('I try to find my scalr objects through API')
def find(ctx):
    ctx.all_entities = dict()
    ctx.all_entities['farms'] = ctx.api.find_farms()
    ctx.all_entities['roles'] = ctx.api.find_roles()
    ctx.all_entities['servers'] = ctx.api.find_servers()
    ctx.all_entities['farm_roles'] = ctx.api.find_farm_roles()
    ctx.all_entities['gvs'] = ctx.api.find_global_variables()

@behave.then('I receive exactly what I added before')
def match(ctx):
    for k,v in ctx.all_entities.items():
        globals()[k] = v

    def compare_two_dicts(d1, d2):
        assert json.dumps(d1, sort_keys=True) == json.dumps(d2, sort_keys=True)

    assert 1 == len(farms)
    compare_two_dicts(farms[0], ctx.farm)

    assert 1 == len(roles)
    compare_two_dicts(roles[0], ctx.role)

    assert 1 == len(farm_roles)
    compare_two_dicts(farm_roles[0], ctx.farm_role)

    assert 2 == len(servers)
    for server in ctx.servers:
        for found_server in servers:
            if server['id'] == found_server['id']:
                compare_two_dicts(server, found_server)
                break
        else:
            raise Exception('Server id={} not found in DB'.format(server['id']))

    print(gvs)
    assert 3 == len(gvs)
