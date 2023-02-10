import os
import time

from flask import Flask


app = Flask(__name__)

_CURRENT_MODULE_PATH = os.path.realpath(__file__)


def start():
    pid1 = os.fork()
    if pid1 == 0:
        os.setsid()
        time.sleep(0.2)
    else:
        os.waitpid(pid1, 0)


@app.route('/')
def index():
    start()
    return "OK"
