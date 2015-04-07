import base64
import OpenSSL

def keygen(length=10):
    unfiltered = base64.b64encode(OpenSSL.rand.bytes(int(length + length*0.2))).decode()
    return ''.join([l for l in unfiltered if l not in ['/', '+']])[:length]