.PHONY: build shell

build:
	docker build -t missioncontrol-v2 .

shell:
	docker run -v $(PWD):/mc-etl -it missioncontrol-v2 /bin/bash
