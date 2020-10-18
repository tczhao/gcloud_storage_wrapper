GCSWRAPPER_VERSION ?= v0.1
RELEASE_BUCKET ?= temp

build:
	rm -rf dist src setup.cfg
	echo "[metadata]" > setup.cfg
	echo "version = $(GCSWRAPPER_VERSION)" >> setup.cfg
	echo "download_url = $(RELEASE_BUCKET)/gcswrapper/" >> setup.cfg
	python3 setup.py sdist

install:
	pip install dist/gcwrapper-$(GCSWRAPPER_VERSION).tar.gz