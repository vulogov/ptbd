__author__ = 'Vladimir Ulogov'

from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

setup(
    ext_modules = cythonize([Extension("whitepy", ["whitepy.pyx"], libraries=["wgdb"])])
)
