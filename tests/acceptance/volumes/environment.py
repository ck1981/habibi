__author__ = 'spike'

import os
import random
import tempfile
import shutil
import mongoengine as me


def before_all(context):
    context.db_name = db_name = 'test%d' % random.randint(1, 1000000)
    me.connect(db_name)
    context.tempdir = tempdir = tempfile.mkdtemp()
    os.chmod(tempdir, 0x777)


def after_scenario(context, scenario):
    try:
        context.server.terminate()
    except:
        pass

    shutil.rmtree(context.tempdir)