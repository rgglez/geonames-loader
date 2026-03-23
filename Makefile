.PHONY: test test-download test-upload

test: test-download test-upload

test-download:
	pytest download/test_download_geonames.py -v

test-upload:
	pytest upload/test_load_geonames.py -v
