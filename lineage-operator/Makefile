# Image URL to use all building/pushing image targets
IMG ?= lineage-operator:latest

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

.PHONY: all
all: build

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: fmt vet ## Run tests.
	go test ./... -coverprofile cover.out

##@ Build

.PHONY: build
build: fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: fmt vet ## Run a controller from your host.
	go run cmd/main.go

.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	docker build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	docker push ${IMG}

##@ Deployment

.PHONY: deploy
deploy: ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	kubectl apply -f config/crd/agentcard_crd.yaml
	kubectl apply -f config/rbac/service_account.yaml
	kubectl apply -f config/rbac/role.yaml
	kubectl apply -f config/rbac/role_binding.yaml
	kubectl apply -f config/manager/deployment.yaml

.PHONY: undeploy
undeploy: ## Undeploy controller from the K8s cluster specified in ~/.kube/config.
	kubectl delete -f config/manager/deployment.yaml --ignore-not-found=true
	kubectl delete -f config/rbac/role_binding.yaml --ignore-not-found=true
	kubectl delete -f config/rbac/role.yaml --ignore-not-found=true
	kubectl delete -f config/rbac/service_account.yaml --ignore-not-found=true
	kubectl delete -f config/crd/agentcard_crd.yaml --ignore-not-found=true

##@ Build Dependencies

.PHONY: deps
deps: ## Download dependencies
	go mod download
	go mod tidy

##@ OpenShift

.PHONY: openshift-setup
openshift-setup: ## Setup BuildConfig and ImageStream on OpenShift
	oc create namespace lineage-operator-system --dry-run=client -o yaml | oc apply -f -
	oc apply -f config/openshift/buildconfig-binary.yaml

.PHONY: openshift-build
openshift-build: ## Build operator image on OpenShift from local source
	oc start-build lineage-operator --from-dir=. --follow -n lineage-operator-system

.PHONY: openshift-push
openshift-push: ## Build locally and push to OpenShift registry (avoids Docker Hub rate limits)
	./push-to-openshift.sh

.PHONY: openshift-deploy
openshift-deploy: ## Deploy operator to OpenShift cluster
	oc apply -f config/crd/agentcard_crd.yaml
	oc apply -f config/rbac/service_account.yaml
	oc apply -f config/rbac/role.yaml
	oc apply -f config/rbac/role_binding.yaml
	oc apply -f config/openshift/deployment-openshift.yaml

.PHONY: openshift-undeploy
openshift-undeploy: ## Undeploy operator from OpenShift cluster
	oc delete -f config/openshift/deployment-openshift.yaml --ignore-not-found=true
	oc delete -f config/rbac/role_binding.yaml --ignore-not-found=true
	oc delete -f config/rbac/role.yaml --ignore-not-found=true
	oc delete -f config/rbac/service_account.yaml --ignore-not-found=true
	oc delete -f config/crd/agentcard_crd.yaml --ignore-not-found=true

.PHONY: openshift-full
openshift-full: openshift-setup openshift-build openshift-deploy ## Complete OpenShift deployment (setup, build, deploy)

.PHONY: openshift-push-deploy
openshift-push-deploy: openshift-push openshift-deploy ## Build locally, push to registry, and deploy (avoids Docker Hub limits)
