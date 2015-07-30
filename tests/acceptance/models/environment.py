import os
import tempfile
import shutil

def before_all(ctx):
    ctx.base_dir = tempfile.mkdtemp()

def after_all(ctx):
    if os.path.isdir(ctx.base_dir):
        shutil.rmtree(ctx.base_dir)