include ../../vars.make

all:
	@docker run --rm --name ${PROJECT_NAME}-compiler -v ${PROJECT_DIR}:/go/src/${PROJECT_NAME} \
		-e GOOS=linux -e CGO_ENABLED=0 -e GOARCH=amd64 -w /go/src/${PROJECT_NAME}/cmd/gosdriver ${BUILD_CONTAINER} \
		 go build -v -buildvcs=false -ldflags \
		"-X 'github.com/cloudian/cloudian-gosbench/internal/gosbench/driver.SelfVersion=$(VERSION)'"
	mv ./gosdriver $(BUILD)/bin/

clean:
	rm -f ./gosdriver