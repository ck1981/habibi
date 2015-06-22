__author__ = 'Nick Demyanchuk'

import json
import uuid
import socket
import string
import functools

import docker
import docker.utils as docker_utils
from playhouse import shortcuts

import habibi.db as habibi_db
from habibi.utils import crypto



class HabibiApiException(Exception):
    pass


class HabibiApi:

    def __init__(self, db_url='sqlite:///habibi.db', docker_url='unix://var/run/docker.sock'):
        habibi_db.connect_to_db(db_url)
        self.docker = docker.Client(base_url=docker_url)

    def _get_many(self, model, *ids):
        objects = model.select().where(model.id << ids)
        if ids and len(ids) != len(objects):
            raise HabibiApiException('Not all {} were found by ids {}'.format(plural, ids))
        return objects

    def _get(self, model, model_id):
        object = model.select().where(model.id == model_id)

    def __getattr__(self, item):
        if item.startswith('get_'):
            model_name = item[4:].replace('_', '').capitalize()
            method = self._get

            if model_name.endswith('s'):
                model_name = model_name[:-1]
                method = self._get_many

            model = getattr(habibi_db, model_name)
            return functools.partial(method, model)

    # Add handling of Index violations
    def create_farm(self, name, farm_crypto_key=None):
        new_farm = habibi_db.Farm.create(name=name)
        return shortcuts.model_to_dict(new_farm)

    def create_role(self, name, image, behaviors=None):
        """Create new habibi role
        For more info see https://scalr-wiki.atlassian.net/wiki/display/docs/Roles

        :type behaviors: str | list | tuple
        """
        behaviors = behaviors or "base"
        if isinstance(behaviors, (list, tuple)):
            behaviors = ','.join(behaviors)

        new_role = habibi_db.Role.create(name=name, image=image, behaviors=behaviors)
        return shortcuts.model_to_dict(new_role)

    def add_farmrole(self, farm_id, role_id, orchestration=None):
        orchestration = orchestration or dict()

        farm = self.get_farms(farm_id)[0]
        role = self.get_roles(role_id)[0]

        new_farmrole = habibi_db.Farmrole.create(farm, role, orchestration)
        return shortcuts.model_to_dict(new_farmrole)

    add_farm_role = add_farmrole

    def create_server(self, farm_role_id, zone=None, volumes=None):
        """Creates server record in DB.

        :param farmrole_id:
        :param zone:
        :param volumes:

        :return:
        """
        farmrole = self.get_farmroles(farmrole_id)[0]
        server_id = str(uuid.uuid4())
        if isinstance(volumes, dict):
            volumes = json.dumps(volumes)

        new_server = habibi_db.Server.create(id=server_id, farmrole=farmrole, zone=zone, volumes=volumes)
        return shortcuts.model_to_dict(new_server)

    def run_server(self, server_id, cmd, env=None):
        """Run docker container for the corresponding server,
        created earlier using `create_server`

        :param server_id:
        :param cmd:
        :param env:

        :return:
        """
        server = self.get_server(server_id)
        volumes = json.loads(server.volumes)

        create_result = self.docker.create_container(server.farmrole.role.image, command=cmd,
             environment=env, volumes=volumes, detach=True, tty=True)
        container_id = create_result['Id']
        self.docker.start(container=container_id)

        habibi_db.Server.update(host_machine=socket.gethostname(),
                                container_id=container_id,
                                status='pending').where(habibi_db.Server.id == server_id)

    def terminate_server(self, server_id):
        server = self.get_server(server_id)
        self.docker.kill(server.container_id)
        self.docker.remove_container(server.container_id)

        habibi_db.Server.update(status='terminated').where(habibi_db.Server.id == server_id)

    def get_server_output(self, server_id):
        pass

    def orchestrate_event(self, event_id):
        event = self.get_events(event_id)[0]
        server = event.triggering_server
        farm_role = server.farm_role
        farm = farm_role.farm
        orcs = farm_role.orchestration.get(event.name, [])

        servers = []
        for fr in farm.farm_roles:
            for s in fr.servers:
                s.farm_role_id = fr.id
                s.behaviors = set(fr.role.behaviors)
                servers.append(s)

        matched_rules = []
        mapping = {}

        for orc_rule in orcs:
            target = orc_rule['target']
            sids = []
            if target['type'] == 'triggering-server':
                sids.append(server.id)
            elif target['type'] == 'behavior':
                sids += [s.id for s in servers
                        if set(target['behaviors']) | s.behaviors]
            elif target['type'] == 'farm-role':
                sids += [s.id for s in servers
                        if s.farm_role_id in target['farm_roles']]
            elif target['type'] == 'farm':
                sids += [s.id for s in servers]

            if sids:
                matched_rules.append(orc_rule)
                rule_index = len(matched_rules) - 1
                for sid in sids:
                    if not sid in mapping:
                        mapping[sid] = []
                    mapping[sid].append(rule_index)
        return {
            'rules': matched_rules,
            'mapping': [{'server_id': server_id,
                        'rule_indexes': mapping[server_id]} for server_id in mapping]
        }

    def create_event(self, name, triggering_server_id, event_id=None):
        server = self.get_server(triggering_server_id)
        event_id = event_id or str(uuid.uuid4())
        new_event = habibi_db.Event.create(name=name, triggering_server=server, event_id=event_id)
        return shortcuts.model_to_dict(new_event)

    def set_global_variable(self, gv_name, gv_value, scope, scope_id):
        """Available scopes: role, farm, farm-role, server."""
        if not scope in habibi_db.GlobalVariable.scopes_available:
            raise HabibiApiException("{} is not correct scope for global variables.".format(scope))

        try:
            getattr(self, 'get_{0}'.format(scope))(scope_id)
        except HabibiApiException:
            raise HabibiApiException('{} with id {} does not exist'.format(scope, scope_id))

        kwargs = {'set__scopes__{0}__scope_{1}'.format(scope, scope_id): gv_value, 'upsert': True}
        habibi.GlobalVariable.objects(name=gv_name).update_one(**kwargs)

    def get_global_variables(self, scope, scope_ids, event_id=None, user_defined=False):
        """
        :param scope: Scope you want to get GVs for
        :param scope_ids: Id or list of ids of the scope
        :param user_defined: if set, return value will contain user defined variables
        :return: Dictionary, {scope_id: {gv_name: value, gv_name2: value}, scope_id2: {gv_name: value}}
        """
        if not isinstance(scope_ids, (list, tuple)):
            scope_ids = [scope_ids]

        if not scope in habibi_db.GlobalVariable.scopes:
            raise HabibiApiException("Unknown GV scope: {0}".format(scope))

        def to_str(value):
            if value is not None:
                return str(value)
            else:
                return ''

        # Mapping {scope_id1: {gv1: value, gv2: value}, scope_id2: {...}}
        gvs = {_id: dict() for _id in scope_ids}

        global_vars_models = habibi.GlobalVariable.objects()

        if scope == 'server':
            server_models = self.get_servers(*scope_ids)
            for server in server_models:
                # Add server-scoped variables
                gvs[server.id].update(dict(
                    SERVER_CLOUD_LOCATION_ZONE=server.zone,
                    SCALR_BEHAVIORS=','.join(server.farm_role.role.behaviors),
                    SCALR_FARM_ROLE_ID=server.farm_role.id,
                    SCALR_FARM_ID=server.farm_role.farm.id,
                    SCALR_SERVER_ID=server.id,
                    SCALR_INSTANCE_INDEX=server.index,
                    SCALR_INTERNAL_IP=server.private_ip,
                    SCALR_EXTERNAL_IP=server.public_ip
                ))

            if event_id:
                event = self.get_events(event_id)[0]
                for server in server_models:
                    gvs[server.id].update(dict(
                        SCALR_EVENT_NAME=event.name,
                        SCALR_EVENT_AVAIL_ZONE=event.triggering_server.zone,
                        SCALR_EVENT_EXTERNAL_IP=event.triggering_server.public_ip,
                        SCALR_EVENT_INTERNAL_IP=event.triggering_server.private_ip,
                        SCALR_EVENT_ROLE_NAME=event.triggering_server.farm_role.role.name,
                        SCALR_EVENT_INSTANCE_INDEX=event.triggering_server.index,
                        SCALR_EVENT_BEHAVIORS=','.join(event.triggering_server.farm_role.role.behaviors),
                        SCALR_EVENT_INSTANCE_ID=event.triggering_server.id,
                        SCALR_EVENT_AMI_ID=event.triggering_server.farm_role.role.image
                    ))

        if not user_defined:
            result = {}
            for server_id, server_gvs in gvs.items():
                result[server_id] = [{'name': key, 'value': to_str(value), 'private': 0}
                        for key, value in server_gvs.items()]
            return result

        # We may easily extend it (add environment and account scopes)
        scopes_graph = {
            'server': {'farm_role': 1},
            'farm_role': {'farm': 2, 'role': 1}
        }

        processed_scopes = list()

        def update_vars_from_scope(scope, scope_id, model=None):
            if scope in processed_scopes:
                return

            if model is None:
                try:
                    model = getattr(self, "get_{0}s".format(scope))(scope_id)[0]
                except KeyError:
                    raise HabibiApiException('{0} with id {1} does not exist'.format(scope.capitalize(), scope_id))

            for gv in global_vars_models:
                if gv.name in gvs:
                    # GV with such name was redefined on lower scope
                    continue
                try:
                    value_for_scope = gv['scopes'][scope]["scope_{0}".format(scope_id)]
                except KeyError:
                    continue

                gvs[gv.name] = value_for_scope

            processed_scopes.append(scope)
            if scope in scopes_graph:
                parents = sorted(scopes_graph[scope].items(), key=lambda x: x[1], reverse=True)

                for parent, weight in parents:
                    parent_id = getattr(model, parent).id
                    update_vars_from_scope(parent, parent_id)

        update_vars_from_scope(scope, scope_id, model=scope_model)
        return [{'name': key, 'value': to_str(value), 'private': 0}
                for key, value in gvs.items()]