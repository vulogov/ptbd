__author__ = 'Vladimir Ulogov'

from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

def whited():
    setup(
        ext_modules = cythonize([Extension("whited", ["whited.pyx"], libraries=[])])
    )

whited()
