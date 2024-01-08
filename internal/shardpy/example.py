import ctypes
import numpy as np
import os

# ---------------------------
# Load the shared library
shardpy_path = "./shardpy.so"
if not os.path.isfile(shardpy_path):
    print("shardpy.so not found, please compile it first")
    print("Current working directory", os.getcwd())
    exit(1)
shardpy = ctypes.cdll.LoadLibrary(shardpy_path)
# ---------------------------
# Go funcs
# func init(dataset *C.char, metric *C.char, vectorSize int)
# func fit(X []float32)
# func query(x []float32, k int, out []uint32)

# Go slices actually have the len and cap fields instead of just being a raw
# pointer. Hence we need to define a GoSlice struct to pass to the Go funcs.
class GoSlice(ctypes.Structure):
    _fields_ = [
        ("data", ctypes.c_void_p),
        ("len", ctypes.c_int64),
        ("cap", ctypes.c_int64)
    ]

shardpy.initShard.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
shardpy.fit.argtypes = [GoSlice]
shardpy.query.argtypes = [GoSlice, ctypes.c_int, GoSlice]
# ---------------------------

dataset = "mydataset"
metric = "euclidean"
vectorSize = 128

shardpy.initShard(dataset.encode("utf-8"), metric.encode("utf-8"), vectorSize)

X = np.random.rand(10000, 128).astype(np.float32)
X = X.flatten()
shardpy.fit(GoSlice(ctypes.cast(X.ctypes.data, ctypes.c_void_p), X.shape[0], X.shape[0]))

x = np.random.rand(128).astype(np.float32)
k = 10
out = np.zeros(k, dtype=np.uint32)

# shardpy.startProfile()
shardpy.query(GoSlice(ctypes.cast(x.ctypes.data, ctypes.c_void_p), x.shape[0], x.shape[0]), k, GoSlice(ctypes.cast(out.ctypes.data, ctypes.c_void_p), out.shape[0], out.shape[0]))
# shardpy.stopProfile()

print(out)
