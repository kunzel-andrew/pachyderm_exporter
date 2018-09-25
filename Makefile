all: lint test build-docker

lint:
	./lint.sh

fmt:
	gofmt -w $$(find * -name '*.go' | grep -v 'vendor')

test:
	go test -v ./...

build-docker:
	docker build -t pachyderm-exporter .

release-docker: build-docker
	docker tag pachyderm-exporter:latest button/pachyderm-exporter:$$(<VERSION)
	docker tag pachyderm-exporter:latest button/pachyderm-exporter:latest
	docker push button/pachyderm-exporter:$$(<VERSION)
	docker push button/pachyderm-exporter:latest
	git tag -a $$(<VERSION) -m "release version $$(<VERSION)"
	git push origin $$(<VERSION)
