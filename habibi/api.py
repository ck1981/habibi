# -*- coding: utf-8 -*-
"""

    habibi.api
    ~~~~~~~~~~

    This module contains HabibiApi class, containing methods
    for running and configuring habibi.

"""
import os
import json
import uuid
import types
import socket
import string
import itertools
import functools
import collections

import docker
import peewee
from playhouse import shortcuts
from docker import utils as docker_utils

import habibi.db
import habibi.exc
from habibi.utils import crypto


class MetaReturnDicts(type):
    def __new__(meta, class_name, bases, class_dict):

        def _wrapper(fn):
            @functools.wraps(fn)
            def wrapped(*args, **kwargs):
                res = fn(*args, **kwargs)
                if isinstance(res, peewee.Model):
                    return shortcuts.model_to_dict(res, backrefs=True)

                elif isinstance(res, collections.Iterable):
                    if all(map(isinstance, res, itertools.repeat(peewee.Model))):
                        return (shortcuts.model_to_dict(x, backrefs=True) for x in res)
                return res

            return wrapped

        new_class_dict = class_dict.copy()
        for attribute_name, attribute_value in class_dict.items():
            if (type(attribute_value) is types.FunctionType):
                if attribute_name.startswith('_'):
                    continue
                new_class_dict[attribute_name] = _wrapper(attribute_value)

        return type.__new__(meta, class_name, bases, new_class_dict)



class HabibiApi(metaclass=MetaReturnDicts):
    """Graph of GV scopes precedence (easily extendable)."""
    scopes_graph = {'server': {'farm_role': 1}, 'farm_role': {'farm': 2, 'role': 1}}

    def __init__(self, db_url='sqlite:///:memory:', docker_url='unix://var/run/docker.sock', base_dir='.habibi'):
        self.base_dir = base_dir
        if not os.path.isdir(base_dir):
            os.makedirs(base_dir)

        self.database = habibi.db.connect_to_db(db_url)
        self.docker = docker.Client(base_url=docker_url)

    def _find_entities(self, model, *ids, **kwargs):
        """Find entities of `model` kind.

           If `ids` provided, filter entities by id, using values from the list.
           If `kwargs` provided, should contain clauses to filter result.

           If neither `ids` nor `kwargs` provided, return all `model` entities.

           :param model: model of sought-for entity
           :type  model: peewee.Model
           :rtype: list of dicts
           :raises habibi.exc.HabibiApiNotFound: if no entities were found
        """
        query = model.select()
        if ids:
            query.where(model.id.in_(ids))
        if kwargs:
            query.filter(**kwargs)

        objects = tuple(query)

        if not objects:
            raise habibi.exc.HabibiApiNotFound(model, self.ids, self.kwargs)

        return objects

    def __getattr__(self, item):
        """Get single habibi entity by id
           or find multiple habibi entities (farms, server, roles, events).

           For `find` method, model name may be specified using
           plural form (for better readability).

           Examples:
               api.get_farm(1)
               api.find_farms(1, 5, 7)
               api.find_roles()
               api.find_servers(zone='us-central1-a')
        """
        if item.startswith(('get_', 'find_')):
            method, scope = item.split('_', 1)
            plural = 'find_' == method

            for maybe_name in (scope, scope[:-1]):
                try:
                    model = habibi.db.get_model_from_scope(maybe_name)
                    break
                except habibi.exc.HabibiModelNotFound as e:
                    pass
            else:
                raise habibi.exc.HabibiApiException('Unknown habibi entity "{}"'.format(scope))

            def search_fn(model, *args, **kwargs):
                ret = self._find_entities(model, *args, **kwargs)
                ret = [shortcuts.model_to_dict(_obj, backrefs=True) for _obj in ret]
                if not plural:
                    return ret[0]
                return ret

            return search_fn

    def create_farm(self, name):
        """Create new farm, and name it as you asked.

            :type name: string
            :param name: Unique name for the new farm.

            :return: Dictionary, containing new farm's attributes with the values
            :rtype: dict
        """
        return habibi.db.Farm.create(name=name)

    def create_role(self, name, image, behaviors=None):
        """Create new habibi role.
           For more info see https://scalr-wiki.atlassian.net/wiki/display/docs/Roles

        :type behaviors: str or iterable
        """
        behaviors = (behaviors is not None) and behaviors or ["base"]
        return habibi.db.Role.create(name=name, image=image, behaviors=behaviors)

    def farm_add_role(self, farm_id, role_id, orchestration=None):
        """Add role to farm, which results in creating farm_role."""
        orchestration = orchestration or dict()
        return habibi.db.FarmRole.create(farm=farm_id, role=role_id, orchestration=orchestration)

    def farm_remove_role(self, farm_id, farm_role_id):
        """
        Remove role from the farm, destroy role's servers.
        :raises HabibiApiNotFound: if farm with id=`farm_id` contains no farm_role with id=`farm_role_id`
        """
        pass
        #FarmRole.delete(farm_role_id=)

    def farm_terminate(self, farm_id):
        pass

    def create_server(self, farm_role_id, zone=None, volumes=None):
        """Creates server record in DB.

        :param farmrole_id:
        :param zone:
        :param volumes:

        :return:
        """
        farm_role = self.get_farm_role(farm_role_id)
        server_id = str(uuid.uuid4())
        if isinstance(volumes, dict):
            volumes = json.dumps(volumes)

        new_server = habibi.db.Server.create(
            id=server_id, farmrole=farmrole, zone=zone, volumes=volumes)
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

        habibi.db.Server.update(host_machine=socket.gethostname(),
                                container_id=container_id,
                                status='pending').where(habibi.db.Server.id == server_id)

    def find_servers(self, **kwargs):
        """

        """
        search_res = []
        if role_name is not None:
            for role in self.roles:
                if role.name == role_name:
                    search_res = copy.copy(role.servers)
                    break
        else:
            search_res = list(itertools.chain(*[r.servers for r in self.roles]))

        if server_id is not None:
            search_res = filter(lambda x: x.id == server_id, search_res)

        if kwds:
            def filter_by_attrs(server):
                for find_attr, find_value in kwargs.iteritems():
                    real_value = getattr(server, find_attr)
                    if callable(find_value):
                        if not find_value(real_value):
                            return False
                    else:
                        if real_value != find_value:
                            return False
                else:
                    return True

            search_res = filter(filter_by_attrs, search_res)

        if search_res:
            return search_res
        else:
            raise LookupError('No servers were found')

    def terminate_server(self, server_id):
        server = self.get_server(server_id)

        self.docker.kill(server.container_id)
        self.docker.remove_container(server.container_id)

        habibi.db.Server.update(status='terminated').where(
            habibi.db.Server.id == server_id)

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
        new_event = habibi.db.Event.create(
            name=name, triggering_server=server, event_id=event_id)
        return shortcuts.model_to_dict(new_event)

    def set_global_variable(self, gv_name, gv_value, scope, scope_id):
        """Available scopes: role, farm, farm-role, server."""
        if scope not in habibi.db.GV_SCOPES_AVAILABLE:
            raise HabibiApiException(
                "Unknown scope for GVs (global variables): {}.".format(scope))

        getattr(self, 'get_{0}'.format(scope))(scope_id)

        with self.database.atomic() as transaction:
            try:
                gv = self.get_global_variable(name=gv_name)
                gv.scopes[scope][scope_id] = gv_value
                gv.save()
            except HabibiNotFound as e:
                scopes = habibi.db.GlobalVariable.scopes.default
                scopes[scope][scope_id] = gv_value
                habibi.db.GlobalVariable.create(name=gv_name, scopes=scopes)


    def calculate_global_variables(self, scope, scope_ids, event_id=None, user_defined=False):
        """Get global variables for the specified scope (e.g. for farm with id=135)

        If scope is `server`, returning value will contain general server-related GVs.
        If both `server` and `event_id` were specified, returning value will also contain event-related GVs.

        If `user_defined` is True, returning value will contain user-defined GVs (created using set_global_variable method),
        overrided down to the `scope`. More about GVs, it's scopes and GVs precedence, see

        https://scalr-wiki.atlassian.net/wiki/display/docs/Global+Variable+Scopes

        :param scope: Scope you want to get GVs for
        :param scope_ids: Id or list of ids of the scope
        :param user_defined: if set, return value will contain user defined variables
        :return: Dictionary, {scope_id: {gv_name: value, gv_name2: value}, scope_id2: {gv_name: value}}
        """
        if not isinstance(scope_ids, (list, tuple)):
            scope_ids = [scope_ids]

        if not scope in habibi.db.GV_SCOPES_AVAILABLE:
            raise HabibiApiException("Unknown scope for GVs (global variables): {}".format(scope))
        scope_model = habibi.db.get_model_from_scope(scope)

        def to_str(value):
            return value is None and '' or str(value)

        # Mapping {scope_id1: {gv1: value, gv2: value}, scope_id2: {...}}
        gvs = {_id: dict() for _id in scope_ids}
        global_vars_list = self.find_global_variables()

        if scope == 'server':
            server_models = self.find_servers(*scope_ids)
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
                event = self.get_event(event_id)
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

        def update_vars_from_scope(scope, scope_id, model=None):
            if scope in processed_scopes:
                return

            if model is None:
                model = getattr(self, "get_{0}".format(scope))(scope_id)

            for gv in global_vars_list:
                if gv.name in gvs:
                    # GV with such name was redefined on lower scope
                    continue
                try:
                    value_for_scope = gv.scopes[scope][scope_id]
                except KeyError:
                    continue

                gvs[gv.name] = value_for_scope

            processed_scopes.append(scope)
            if scope in self.scopes_graph:
                parents = sorted(
                    self.scopes_graph[scope].items(), key=lambda x: x[1], reverse=True)

                for parent, weight in parents:
                    parent_id = getattr(model, parent).id
                    update_vars_from_scope(parent, parent_id)

        if user_defined:
            scope_ids = map(str, scope_ids)
            processed_scopes = list()
            for scope_id in scope_ids:
                update_vars_from_scope(scope, scope_id, model=scope_model)

        return [{'name': key, 'value': to_str(value), 'private': 0}
                for key, value in gvs.items() if value]
