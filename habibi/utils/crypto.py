import os
import string
import random

def keygen(length=10):
    avail_chars = string.letters + string.digits + string.punctuation
    random.seed = os.urandom(1024)

    return ''.join([random.choice(avail_chars) for _ in range(length)])
