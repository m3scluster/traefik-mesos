#Dockerfile vars

#vars
IMAGENAME=traefik_mesos
TAG=v3.7.10
UPDATE=-1
BRANCH=${TAG}${UPDATE}
BRANCHSHORT=$(shell echo ${BRANCH} | awk -F. '{ print $$1"."$$2 }')
IMAGEFULLNAME=avhost/${IMAGENAME}
BUILDDATE=$(shell date -u +%Y%m%d)
VERSION_TU=$(subst -, ,$(TAG:v%=%))	
BUILD_VERSION=$(word 1,$(VERSION_TU))
LASTCOMMIT=$(shell git log -1 --pretty=short | tail -n 1 | tr -d " " | tr -d "UPDATE:")

.PHONY: help build build-docker clean all

help:
	    @echo "Makefile arguments:"
	    @echo ""
	    @echo "Makefile commands:"
	    @echo "build"
			@echo "build-docker"
	    @echo "all"
			@echo ${TAG}

.DEFAULT_GOAL := all

clone: 
	@if [ ! -d "traefik_repo" ] ; then \
		git clone https://github.com/traefik/traefik.git traefik_repo; \
	fi
	cd traefik_repo;	git checkout $(TAG)

patch:
	patch -u traefik_repo/pkg/config/static/static_config.go -i static_config.patch
	patch -u traefik_repo/pkg/provider/aggregator/aggregator.go -i aggregator.patch
	patch -u traefik_repo/webui/src/components/icons/providers/index.tsx -i ProviderIcon.patch
	cp -pr mesos traefik_repo/pkg/provider/

build: 
	@echo ">>>> Build traefik executable ${BUILD_VERSION}"
	cp -pr mesos traefik_repo/pkg/provider/
	@cd traefik_repo; go get -d 
	@cd traefik_repo; go get github.com/mesos/mesos-go/api/v0/detector/zoo
	@cd traefik_repo; go mod tidy
	cp static/Mesos.tsx traefik_repo/webui/src/components/icons/providers
	cd traefik_repo; $(MAKE) generate-webui
	export VERSION=${BUILD_VERSION}; cd traefik_repo; $(MAKE)

build-docker: build
	@echo ">>>> Build docker image" ${BRANCH}
	docker build -t ${IMAGEFULLNAME}:latest . 

push: build
	@echo ">>>> Publish it to repo" ${BRANCH} ${BUILDSHORT}
	-docker buildx create --use --name buildkit
	@docker buildx build --sbom=true --provenance=true --platform linux/amd64 --push --build-arg VERSION=${TAG} -t ${IMAGEFULLNAME}:${BRANCH} .
	@docker buildx build --sbom=true --provenance=true --platform linux/amd64 --push --build-arg VERSION=${TAG} -t ${IMAGEFULLNAME}:${BRANCHSHORT} .
	@docker buildx build --sbom=true --provenance=true --platform linux/amd64 --push --build-arg VERSION=${TAG} -t ${IMAGEFULLNAME}:latest .
	-docker buildx rm buildkit

clean:
	rm -rf traefik_repo

update-gomod:
	go get -u
	go mod tidy

seccheck:
	grype --add-cpes-if-none .

sboom:
	syft dir:. > sbom.txt
	syft dir:. -o json > sbom.json

imagecheck:
	grype --add-cpes-if-none ${IMAGEFULLNAME}:latest > cve-report.md

check: sboom seccheck imagecheck
all: clone patch build-docker check
