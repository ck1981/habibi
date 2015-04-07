__author__ = 'spike'

import os
import functools

from operator import xor

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import mongoengine as me

import habibi


class ApiException(Exception):
    pass

GV_SCOPES = ('farm', 'role', 'farm_role', 'server')

class ScalrApi(object):

    def __init__(self, mongodb_url=None):
        habibi.configure(mongodb_url)
        self.get_servers = functools.partial(self._get, habibi.Server, 'servers')
        self.get_farms = functools.partial(self._get, habibi.Farm, 'farms')
        self.get_farm_roles = functools.partial(self._get, habibi.FarmRole, 'farm roles')
        self.get_roles = functools.partial(self._get, habibi.Role, 'roles')
        self.get_events = functools.partial(self._get, habibi.Event, 'events')

    def _get(self, model, plural, *ids):
        kwargs = ids and {"id__in": ids} or dict()
        objects = model.objects(**kwargs)
        if ids and len(ids) != len(objects):
            raise ApiException('Not all {0} were found by ids {1}'.format(plural, ", ".join(map(str, ids))))
        return objects


    def get_farm_by_name(self, name):
        try:
            return habibi.Farm.objects(name=name)[0]
        except IndexError:
            raise ApiException('Farm with name {0} not found')

    def create_farm(self, name):
        farm = habibi.Farm(name=name)
        farm.save()
        return farm

    def create_role(self, name, image, behaviors=None):
        behaviors = behaviors or ["base"]
        role = habibi.Role(name=name, image=image, behaviors=behaviors)
        role.save()
        return role


    def add_farm_role(self, farm_id, role_id, orchestration=None):
        orchestration = orchestration or dict()

        farm = self.get_farms(farm_id)[0]
        role = self.get_roles(role_id)[0]

        farm_role = farm.add_farm_role(role, orchestration)
        return farm_role


    def create_server(self, farm_role_id, zone=None, volumes=None):
        farm_role = self.get_farm_roles(farm_role_id)[0]
        server = farm_role.add_server(zone=zone, volumes=volumes)
        return server


    def run_server(self, server_id, cmd, env=None):
        server = self.get_servers(server_id)[0]
        server.run(cmd=cmd, env=env)


    def terminate_server(self, server_id):
        server = self.get_servers(server_id)[0]
        server.terminate()


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

        #farm_role.behaviors = set(farm_role.behaviors)

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

    def create_event(self, name, triggering_server_id):
        server = self.get_servers(triggering_server_id)[0]
        ev = habibi.Event(name=name, triggering_server=server)
        ev.save()
        return ev

    def set_global_variable(self, gv_name, gv_value, scope, scope_id):
        """
        Available scopes: role, farm, farm-role, server
        """
        if not scope in GV_SCOPES:
            raise ApiException("Unknown GV scope: {0}".format(scope))

        try:
            getattr(self, 'get_{0}s'.format(scope))(scope_id)
        except ApiException:
            raise ApiException('{0} with id {1} does not exist'.format(scope, scope_id))

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

        if not scope in GV_SCOPES:
            raise ApiException("Unknown GV scope: {0}".format(scope))

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
                    raise ApiException('{0} with id {1} does not exist'.format(scope.capitalize(), scope_id))

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











        

