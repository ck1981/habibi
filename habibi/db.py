__author__ = 'Nick Demyanchuk'
__email__ = "spike@scalr.com"

import uuid

import peewee
from playhouse import db_url

from habibi.utils import crypto

DB_PROXY = peewee.Proxy()

def connect_to_db(url):
    database = db_url.connect(url)
    DB_PROXY.initialize(database)
    for model in SCALR_ENTITIES:
        model.create_table(fail_silently=True)


"""Peewee models for Scalr entities."""

def db_table_name_for_model(model):
     return "{}s".format(model.__class__.__name__.lower())


class HabibiModel(peewee.Model):
    class Meta:
        database = DB_PROXY
        db_table_func = db_table_name_for_model

class Farm(HabibiModel):
    name = peewee.CharField(unique=True)
    status = peewee.CharField(default='terminated',
                              choices=(('running', 'Running'), ('terminated', 'Terminated')))

    '''
    def add_farm_role(self, role, orchestration=None):
        if isinstance(role, int):
            role = Role.get(id=role)
        return FarmRole.create(farm=self, role=role, orchestration=orchestration)

    @property
    def started(self):
        return self.status == 'running'

    def remove_role(self, role):
        if role not in self.roles:
            raise Exception('Role %s not found' % role.name)
        self.roles.remove(role)
        for server in self.servers(role_name=role.name):
            try:
                server.destroy()
            except:
                LOG.debug('Failed to terminate server %s' % server.id, exc_info=sys.exc_info())

    def start(self):
        if not os.path.isdir(self.base_dir):
            os.makedirs(self.base_dir)
        self.update(status='running').execute()

    def stop(self):
        self.update(status='terminated').execute()

    def servers(self, server_id=None, role_name=None, **kwds):
        """
        @param sid: find server with specified id
        @param role: role name to search servers in
        @param kwds: filter found servers by attribute values (see examples)

        keyword arguments are additional filters for servers, where key is server's attribute name,
        and value either attribute value or validator function, which accepts single argument (attribute value):

            # find pending and initilizing servers across all roles
            servers(status=lambda s: s.status in ('pending', 'initializing'))

            # find server with index=3 in percona55 role
            third = servers(role_name='percona55', index=3)[0]
        """
        self.reload()
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
                for find_attr, find_value in kwds.iteritems():
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
            return ServerSet(search_res)
        else:
            raise LookupError('No servers were found')


    def orchestrate_event(self, event_id):
        pass
    '''

class Role(HabibiModel):
    name = peewee.CharField(unique=True)
    image = peewee.CharField()
    behaviors = peewee.CharField()


class Farmrole(HabibiModel):
    farm = peewee.ForeignKeyField(Farm, related_name='farm_roles')
    role = peewee.ForeignKeyField(Role)
    orchestration = peewee.TextField()

'''
    def _next_server_index(self):
        return random.randint(1, 10000)

    def _pack_user_data(self, user_data):
        return ';'.join(['{0}={1}'.format(k, v) for k, v in user_data.items()])

    def add_server(self, zone='lxc-zone', volumes=None):
        """
        @param volumes: dirs to share with container, host_path: container_path
        @type volumes: dict
        """
        self.reload()
        if not self.farm.started:
            raise Exception("You should start your farm first.")

        server = Server(farm_role=self,
                        behaviors=self.role.behaviors,
                        index=self._next_server_index(),
                        zone=zone,
                        role=self,
                        host_machine=THIS_MACHINE,
                        volumes=volumes)
        server.save()

        self.update(add_to_set__servers=server)
        self.reload()
        return server
'''

class Server(HabibiModel):

    id = peewee.CharField(primary_key=True)
    index = peewee.IntegerField()
    zone = peewee.CharField()
    farmrole = peewee.ForeignKeyField(Farmrole, related_name='servers')
    public_ip = peewee.CharField(null=True)
    private_ip = peewee.CharField(null=True)
    host_machine = peewee.CharField(null=True)
    container_id = peewee.CharField(null=True)
    volumes = peewee.TextField(null=True)
    status = peewee.CharField(default='pending launch',
                              choices=(('running', 'Running'), ('pending launch', 'Pending launch'),
                                       ('pending', 'Pending'), ('initializing', 'Initializing'),
                                       ('pending terminate', 'Pending terminate'), ('terminated', 'Terminated')))
"""
    def __init__(self, farm_role=None, id=None, index=None,
                 public_ip=None, private_ip=None, status='pending launch', zone=None,
                 host_machine=None, volumes=None, **kwargs):
        kwargs2 = dict(id=id or str(uuid.uuid4()),
                       index=index,
                       farm_role=farm_role,
                       public_ip=public_ip,
                       private_ip=private_ip,
                       status=status,
                       zone=zone,
                       host_machine=host_machine,
                       volumes=volumes or dict())
        kwargs.update(kwargs2)
        super(Server, self).__init__(**kwargs)
"""
'''
    def wait(self):
        p = subprocess.Popen(('docker', 'wait', self.container_id), shell=True, stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p.communicate()

    @property
    def server_dir(self):
        return os.path.join(self.farm_role.farm.base_dir, self.id)
'''


class Event(HabibiModel):
    name = peewee.CharField()
    event_id = peewee.CharField()
    triggering_server = peewee.ForeignKeyField(Server, related_name='sent_events')


class GlobalVariable(HabibiModel):
    scopes_available = ('farm', 'role', 'farm_role', 'server')

    name = peewee.CharField()
    scopes = peewee.CharField()

SCALR_ENTITIES = (Farm, Role, Farmrole, Server, Event, GlobalVariable)
