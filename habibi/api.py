# -*- coding: utf-8 -*-
"""

    habibi.api
    ~~~~~~~~~~


"""
import os
import json
import uuid
import types
import socket
import logging
import itertools
import functools
import collections

import six
import docker
import peewee
import playhouse.shortcuts as db_shortcuts

import habibi.db as habibi_db
import habibi.exc as habibi_exc


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
logging.basicConfig()


class MetaReturnDicts(type):
    """Renders peewee.Model to dicts in method results."""
    def __new__(meta, class_name, bases, class_dict):

        def _wrapper(fn):
            @functools.wraps(fn)
            def wrapped(*args, **kwargs):
                res = fn(*args, **kwargs)
                if isinstance(res, peewee.Model):
                    """Return dict instead of peewee.Model."""
                    return db_shortcuts.model_to_dict(res)
                elif isinstance(res, collections.Iterable):
                    if all(map(isinstance, res, itertools.repeat(peewee.Model))):
                        """Transform list of peewee.Model objects to their JSON value."""
                        return (db_shortcuts.model_to_dict(x) for x in res)
                """Return result untouched."""
                return res
            return wrapped

        new_class_dict = class_dict.copy()
        for attribute_name, attribute_value in class_dict.items():
            """Decorate all public methods."""
            if type(attribute_value) is types.FunctionType:
                if attribute_name.startswith('_'):
                    continue
                new_class_dict[attribute_name] = _wrapper(attribute_value)
        return type.__new__(meta, class_name, bases, new_class_dict)


class HabibiApi(six.with_metaclass(MetaReturnDicts, object)):

    _gv_scopes = ('server', 'farm_role', 'farm', 'role')
    _gv_scopes_resolution = {'server': {'farm_role': 1}, 'farm_role': {'farm': 2, 'role': 1}}

    def __init__(self, db_url=None, docker_url=None, base_dir=None):

        self.base_dir = base_dir or '.habibi'
        if not os.path.isdir(self.base_dir):
            os.makedirs(self.base_dir)

        db_url = db_url or 'sqlite:///:memory:'
        self.database = habibi_db.connect_to_db(db_url)

        docker_url = docker_url or 'unix://var/run/docker.sock'
        self.docker = docker.Client(base_url=docker_url)

    def _find_entities(self, model, *ids, **kwargs):
        """Find entities of `model` kind.

           If `ids` provided, filter entities by id, using values from the list.
           If `kwargs` provided, should contain clauses to filter result.

           If neither `ids` nor `kwargs` provided, return all `model` entities.

           :param model: model of sought-for entity
           :type  model: peewee.Model
           :rtype: list of habibi_db.HabibiModel
           :raises habibi_exc.HabibiApiNotFound: if no entities were found
        """
        query = model.select()
        if ids:
            query = query.where(model.id.in_(ids))
        for k, v in six.iteritems(kwargs):
            query = query.where((getattr(model, k) == v))

        objects = list(query)

        if not objects:
            raise habibi_exc.HabibiApiNotFound(model, ids, kwargs)

        return objects

    def __getattr__(self, item):
        """Get single habibi entity by id
           or find multiple habibi entities (farms, server, roles, events).

           For `find` method, model name may be specified using
           plural form (for better readability).

           Examples::
               api.get_farm(1)
               # returns: dict, containing attributes of farm with id=1

               api.find_farms(1, 5, 7)
               # returns list of dicts, that represent found farms. If no farms were found,
               # raises habibi_exc.HabibiApiNotFound exception

               api.find_roles()
               # returns all roles from database

               api.find_servers(zone='us1-a')
               # returns all servers, started in zone 'us1-a'
        """
        if item.startswith(('get_', 'find_')):
            method, scope = item.split('_', 1)
            plural = 'find' == method

            for maybe_name in (scope, scope[:-1]):
                try:
                    model = habibi_db.get_model_from_scope(maybe_name)
                    break
                except habibi_exc.HabibiModelNotFound:
                    continue
            else:
                raise habibi_exc.HabibiApiException('Unknown habibi entity "{}"'.format(scope))

            def search_fn(*args, **kwargs):
                ret = self._find_entities(model, *args, **kwargs)
                if not plural:
                    return db_shortcuts.model_to_dict(ret[0])
                return [db_shortcuts.model_to_dict(_obj) for _obj in ret]

            return search_fn

        raise AttributeError(item)

    def create_farm(self, name):
        """Create new farm and save it to DB.

           :param string name: Unique name for the new farm.
           :returns: JSON representation of new farm.
        """
        return habibi_db.Farm.create(name=name)

    def create_role(self, name, image, behaviors=None):
        """Create new habibi role, save to DB.

        :param string image: docker image name.
        :param list behaviors: list of role's behaviors
        """
        behaviors = (behaviors is not None) and behaviors or ["base"]
        return habibi_db.Role.create(name=name, image=image, behaviors=behaviors)

    def farm_add_role(self, farm_id, role_id, orchestration=None):
        """Add role to farm, which results in creating new farm_role."""
        orchestration = orchestration or dict()
        return habibi_db.FarmRole.create(farm=farm_id, role=role_id, orchestration=orchestration)

    def farm_remove_role(self, farm_id, farm_role_id):
        """
        Remove role from the farm, destroy role's servers.
        :raises HabibiApiNotFound: if farm with id=`farm_id` contains no farm_role with id=`farm_role_id`
        """
        farm_role = self._find_entities(habibi_db.FarmRole, farm_role_id, farm=farm_id)[0]

        for server in farm_role.servers:
            self._terminate_server(server)

        farm_role.delete_instance()

    def farm_terminate(self, farm_id):
        """Set Farm status to 'terminated', terminate all farm's servers."""
        farm = self._find_entities(habibi_db.Farm, farm_id)[0]
        servers = itertools.chain([farm_role.servers for farm_role in farm.farm_roles])
        for server in servers:
            self._terminate_server(server)

        habibi_db.Farm.update(status='terminated').where(id=farm_id).execute()

    def create_server(self, farm_role_id, server_id=None, volumes=None):
        """Creates server record in DB.

        :param zone:
        :param volumes:

        :return:
        """
        server_id = server_id or str(uuid.uuid4())
        volumes = volumes or dict()
        with self.database.atomic():
            latest_index = habibi_db.Server.select(peewee.fn.Max(habibi_db.Server.index)).scalar()
            index_for_new_server = latest_index and (latest_index + 1) or 1
            return habibi_db.Server.create(index=index_for_new_server, id=server_id,
                                           farm_role=farm_role_id, volumes=volumes)

    def run_server(self, server_id, cmd, env=None):
        """Run docker container for the server, created earlier using `create_server`.

        :param list cmd: list of command-line arguments to run inside docker container
        :param dict env: environment variables to set for the container
        """
        server = self._find_entities(habibi_db.Server, server_id)[0]
        binds = ["{}:{}".format(k, v) for k, v in six.iteritems(server.volumes)]


        create_result = self.docker.create_container(server.farm_role.role.image,
            command=cmd, environment=env, detach=True, tty=True,
            host_config=docker.utils.create_host_config(binds=binds),
            volumes=list(six.itervalues(server.volumes)))
        container_id = create_result['Id']
        self.docker.start(container=container_id)

        habibi_db.Server.update(host_machine=socket.gethostname(),
                                container_id=container_id,
                                status='pending').where(habibi_db.Server.id == server_id).execute()

    def terminate_server(self, server_id):
        """Terminate container for the server with specified id."""
        server = self._find_entities(habibi_db.Server, server_id)
        self._terminate_server(server)

    def _terminate_server(self, server):
        if not server['container_id']:
            return

        self.docker.kill(server['container_id'])
        self.docker.remove_container(server['container_id'])
        habibi_db.Server.update(status='terminated').where(habibi_db.Server.id == server['id'])

    def get_server_output(self, server_id):
        """Retrieve output of container for the server with id=`server_id`."""
        server = self.get_server(server_id)
        if not server.get('container_id'):
            raise habibi_exc.HabibiApiException(
                    'Server has not been started yet. server_id={}'.format(server_id))
        return self.docker.logs(server['container_id'])

    def orchestrate_event(self, event_id):
        """Calculate EventOrchestration for the event_id.

           Little explanation: Calculate targets of orchestration rules,
           using rules from FarmRole of the server, that triggered event.

           More detailed info: `https://scalr-wiki.atlassian.net/wiki/display/docs/Orchestration+Rules`

           :type event_id: integer
           :returns: JSON representation of EventOrchestration (see private wiki for more info)
        """
        event = self._find_entities(habibi_db.Event, event_id)[0]
        server = event.triggering_server
        farm_role = server.farm_role
        farm = farm_role.farm
        orcs = farm_role.orchestration.get(event.name, [])

        """Collect all servers of the farm, where event occured."""
        servers = []
        for fr in farm.farm_roles:
            for s in fr.servers:
                if s.status in ('terminated', 'pending terminate', 'pending launch'):
                    continue
                s.farm_role_id = fr.id
                s.behaviors = set(fr.role.behaviors)
                servers.append(s)

        matched_rules = []
        mapping = {}

        for orc_rule in orcs:
            """"""
            target = orc_rule['target']
            sids = []
            if target['type'] == 'triggering-server':
                sids.append(server.id)
            elif target['type'] == 'behavior':
                sids += [s.id for s in servers if set(target['behaviors']) | s.behaviors]
            elif target['type'] == 'farm-role':
                sids += [s.id for s in servers if s.farm_role_id in target['farm_roles']]
            elif target['type'] == 'farm':
                sids += [s.id for s in servers]

            if sids:
                matched_rules.append(orc_rule)
                rule_index = len(matched_rules) - 1
                for sid in sids:
                    if not sid in mapping:
                        mapping[sid] = []
                    mapping[sid].append(rule_index)

        return {'rules': matched_rules,
                'mapping': [{'server_id': server_id, 'rule_indexes': mapping[server_id]}
                    for server_id in mapping]}

    def create_event(self, name, triggering_server_id, event_id=None):
        """Create new event, that was triggered by server."""
        event_id = event_id or str(uuid.uuid4())
        return habibi_db.Event.create(name=name, triggering_server=triggering_server_id, id=event_id)

    def set_global_variable(self, gv_name, gv_value, scope, scope_id):
        """Set value of user-defined GV in the provided scope.
           More about Global Variables:
           `https://scalr-wiki.atlassian.net/wiki/display/docs/Global+Variables`

           :type gv_name: str
           :type gv_value: str
           :type scope: str
           :type scope_id: int
        """
        if scope not in self._gv_scopes:
            raise habibi_exc.HabibiApiException("Unknown scope for GVs (global variables): {}.".format(scope))

        getattr(self, 'get_{0}'.format(scope))(scope_id)

        with self.database.atomic():
            try:
                values_for_scopes = self.get_global_variable(name=gv_name)['scopes']
                values_for_scopes[scope][scope_id] = gv_value
                habibi_db.GlobalVariable.update(scopes=values_for_scopes).where(
                    habibi_db.GlobalVariable.name == gv_name).execute()
            except habibi_exc.HabibiNotFound:
                scopes = {scope: {} for scope in self._gv_scopes}
                scopes[scope][scope_id] = gv_value
                habibi_db.GlobalVariable.create(name=gv_name, scopes=scopes)


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

        if scope not in habibi_db.GV_SCOPES_AVAILABLE:
            raise habibi_exc.HabibiApiException("Unknown scope for GVs (global variables): {}".format(scope))
        scope_model = habibi_db.get_model_from_scope(scope)

        def to_str(value):
            return value is None and '' or str(value)

        # Mapping {scope_id1: {gv1: value, gv2: value}, scope_id2: {...}}
        gvs = {_id: dict() for _id in scope_ids}
        global_vars_list = self.find_global_variables()

        if scope == 'server':
            server_models = self._find_entities(habibi_db.Server, *scope_ids)
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
                event = self._find_entities(habibi_db.Event, event_id)[0]
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

                for parent, _ in parents:
                    parent_id = getattr(model, parent).id
                    update_vars_from_scope(parent, parent_id)

        if user_defined:
            scope_ids = map(str, scope_ids)
            processed_scopes = list()
            for scope_id in scope_ids:
                update_vars_from_scope(scope, scope_id, model=scope_model)

        return [{'name': key, 'value': to_str(value), 'private': 0}
                for key, value in gvs.items() if value]
