.PHONY: compose
compose:
	docker compose build

# This starts mock s3 service for testing
.PHONY: up
up:
	docker compose up

.PHONY: bash
bash:
	docker exec -it collator_lambda /bin/bash

# Requires running aws container (via make up)

.PHONY: test
test:
	docker exec -it collator_lambda python -m pytest test/

.PHONY: pdb
pdb:
	docker exec -it collator_lambda python -m pytest --pdb test/

.PHONY: integration
integration:
	docker exec -it collator_lambda /bin/bash -c 'python -m pytest -m integration test/test_collator.py'
