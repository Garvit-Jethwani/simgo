# Build and deploy SimGo's hosted exploration service.
# See docs/deploy/aws.md for the full walkthrough.

REGION     ?= ap-south-1
TF_DIR     ?= ../deploy/aws
IMAGE_TAG  ?= $(shell git rev-parse --short HEAD)
REPO_URL    = $(shell terraform -chdir=$(TF_DIR) output -raw worker_repository_url)

.PHONY: test lambda image push deploy-infra deploy clean

test:
	go vet ./...
	go test -race ./...

# The API Lambda (provided.al2023, arm64).
lambda:
	mkdir -p dist
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -trimpath -ldflags="-s -w" -o dist/bootstrap ./cmd/simgo-api-lambda
	cd dist && rm -f simgo-api-lambda.zip && zip -q simgo-api-lambda.zip bootstrap

# The worker image (arm64, for Fargate Graviton).
image:
	docker buildx build --platform linux/arm64 -f Dockerfile -t simgo-worker:$(IMAGE_TAG) ..

push: image
	aws ecr get-login-password --region $(REGION) | docker login --username AWS --password-stdin $(firstword $(subst /, ,$(REPO_URL)))
	docker tag simgo-worker:$(IMAGE_TAG) $(REPO_URL):$(IMAGE_TAG)
	docker push $(REPO_URL):$(IMAGE_TAG)

deploy-infra: lambda
	terraform -chdir=$(TF_DIR) init
	terraform -chdir=$(TF_DIR) apply -var region=$(REGION) -var image_tag=$(IMAGE_TAG)

# Create the infrastructure, push the worker image, then point the task
# definition at it.
deploy: deploy-infra push
	terraform -chdir=$(TF_DIR) apply -var region=$(REGION) -var image_tag=$(IMAGE_TAG)

clean:
	rm -rf dist
