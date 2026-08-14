#!/usr/bin/env bash
# 在 Python 3.11 + Linux 上安装 EQ 依赖（Ice 3.6.4 需本地补丁编译）
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV_PY="${ROOT}/.venv/bin/python"
EQ_PKG="${HOME}/MyProject/EQAPI/eqapi_v1.3.6_20220711/api/python/packages"
ICE_TGZ="${EQ_PKG}/zeroc-ice-3.6.4.tar.gz"
EQ_WHL="${EQ_PKG}/eqapi-1.3.6-py3-none-any.whl"

"${VENV_PY}" -m pip install python-snappy psutil
"${VENV_PY}" -m pip install --no-deps "${EQ_WHL}"

BUILD=/tmp/eq_ice36_build
rm -rf "${BUILD}"
mkdir -p "${BUILD}"
tar xf "${ICE_TGZ}" -C "${BUILD}"
cd "${BUILD}/zeroc-ice-3.6.4"
# Python 3.11 patches
sed -i 's|#include <eval.h>|#include <Python.h>|' src/Slice.cpp
sed -i 's|#include <compile.h>|#include <Python.h>|' src/Slice.cpp src/Util.cpp || true
python3 - <<'PY'
from pathlib import Path
p = Path('src/Util.cpp')
text = p.read_text()
old = """    PyFrameObject *f = PyThreadState_GET()->frame;
    PyObjectHandle code = PyObject_GetAttrString(reinterpret_cast<PyObject*>(f), STRCAST(\"f_code\"));"""
new = """#if PY_VERSION_HEX >= 0x030B0000
    PyFrameObject *f = PyThreadState_GetFrame(PyThreadState_GET());
    if(!f) { return string(\"<unknown>\"); }
    PyObjectHandle code = PyObject_GetAttrString(reinterpret_cast<PyObject*>(f), STRCAST(\"f_code\"));
    Py_DECREF(f);
#else
    PyFrameObject *f = PyThreadState_GET()->frame;
    PyObjectHandle code = PyObject_GetAttrString(reinterpret_cast<PyObject*>(f), STRCAST(\"f_code\"));
#endif"""
if old not in text:
    raise SystemExit('Util.cpp patch pattern missing')
p.write_text(text.replace(old, new, 1))
print('patched Util.cpp frame API')
PY
export CFLAGS="-std=gnu17 -Wno-error -Wno-implicit-function-declaration"
export CXXFLAGS="-Wno-error"
"${VENV_PY}" setup.py build_ext -j"$(nproc)"
"${VENV_PY}" setup.py install
"${VENV_PY}" -c "import Ice, eqapi; print('Ice', Ice.stringVersion(), 'eqapi ok')"
