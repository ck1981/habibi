__author__ = 'spike'

import os
import random
import string
import behave


import habibi




@behave.when('i create farm and add role to it')
def add_farm_and_role(context):
    context.farm = habibi.Habibi('test_farm')
    context.farm.save()
    context.farm.start()
    context.role = context.farm.add_role('my_first_role', 'app')

@behave.when('start server with shared directory')
def start_server(context):
    context.server = context.role.add_server(volumes={context.tempdir: '/tmp/testmount'})
    context.file_name = "".join([random.choice(string.letters) for _ in range(10)])
    file_path = '/tmp/testmount/%s' % context.file_name
    context.server.run(broker_url='amqp://real/broker/url/', cmd='touch %s' % file_path)
    context.server.wait()

@behave.then('i am able to see file created from inside')
def file_exists(context):
    assert os.path.isfile(os.path.join(context.tempdir, context.file_name))