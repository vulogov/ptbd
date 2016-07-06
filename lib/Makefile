all: whited.so
clean:
	rm -rf build whited.c whited.so
whited.so: setup.py whited.pyx
	python setup.py build_ext --inplace
	strip whited.so

