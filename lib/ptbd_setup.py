__author__ = 'Vladimir Ulogov'

from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

def whited():
    print "Building main DB module"
    setup(
        ext_modules = cythonize([Extension("whited", ["whited.pyx"], libraries=[])])
    )

def ptbd_util():
    print "Building utilites module"
    setup(
        ext_modules = cythonize([Extension("ptbd_util", ["ptbd_util.pyx"], libraries=[])])
    )


