import os
import sys
import subprocess
from time import time

tests = [
    f[:-3]
    for f in os.listdir(os.path.dirname(__file__))
    if f.endswith(".py") and not f.startswith("__")
]
for test in tests:
    cmd = [sys.executable, "-m", __package__ + "." + test]
    sys.stdout.write("{:60s}".format(" ".join(cmd)))
    sys.stdout.flush()
    start = time()
    subprocess.run(cmd, check=True, stderr=subprocess.STDOUT)
    sys.stdout.write(" succeeded in {:.2f} sec\n".format(time() - start))
