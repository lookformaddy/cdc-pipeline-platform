.PHONY: test demo serve lint

test:
	pytest -q

demo:
	python -m cdc_pipeline demo

serve:
	python -m cdc_pipeline serve

lint:
	python -m compileall -q src tests examples
