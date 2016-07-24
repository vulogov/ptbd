all: whited.so ptbd_util.so
clean:
	rm -rf build whited.c ptbd_util.c *.so
freedb:
	wgdb 10051 free
	wgdb 10052 free
whited.so: ptbd_setup.py whited.pyx
	python -c "import ptbd_setup; ptbd_setup.whited()" build_ext --inplace
	strip whited.so
ptbd_util.so: ptbd_util.pyx ptbd_setup.py
	python -c "import ptbd_setup; ptbd_setup.ptbd_util()" build_ext --inplace
	strip ptbd_util.so

