all: lint test build-docker

lint:
	./lint.sh

fmt:
	gofmt -w $$(find * -name '*.go' | grep -v 'vendor')

test:
	go test -v ./...

build-docker:
	docker build -t pachyderm_exporter .

release-docker: build-docker
	docker tag pachyderm_exporter:latest button/pachyderm_exporter:$$(<VERSION)
	docker push button/pachyderm_exporter:$$(<VERSION)
