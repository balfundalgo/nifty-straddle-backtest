# hook-pyarrow.py — PyInstaller runtime hook for pyarrow
# Forces pyarrow to initialize before pandas tries to use it
import os
import sys

# Ensure pyarrow DLLs can be found
if hasattr(sys, '_MEIPASS'):
    arrow_path = os.path.join(sys._MEIPASS, 'pyarrow')
    if os.path.exists(arrow_path):
        os.environ['ARROW_HOME'] = arrow_path

try:
    import pyarrow
    import pyarrow.lib
    import pyarrow.pandas_compat
except Exception:
    pass
