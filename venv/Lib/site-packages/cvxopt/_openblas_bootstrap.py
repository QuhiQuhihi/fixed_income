
'''
OpenBLAS bootstrap to preload Windows DLL in order to avoid ImportError.
'''
import os
import threading

_openblas_loaded = threading.Event()

def _load_openblas_win():

    if _openblas_loaded.is_set(): 
        return

    old_path = os.environ['PATH']
    lib_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '.lib')
    try:
        os.add_dll_directory(lib_path)
    except:
        os.environ['PATH'] = os.pathsep.join([lib_path, old_path])

    from ctypes import cdll
    try:
        cdll.LoadLibrary(os.path.join(lib_path, 'libopenblas.dll'))
    except OSError as e:
        raise ImportError("Cannot find OpenBLAS") from e
    else:
        _openblas_loaded.set()
    finally:
        os.environ['PATH'] = old_path

if os.name == 'nt':
    _load_openblas_win()
