include ../../vars.make

all:
	sudo docker run --rm --name ${PROJECT_NAME}-compiler -v ${PROJECT_DIR}:/go/src/${PROJECT_NAME} \
		-e GOOS=linux -e CGO_ENABLED=0 -e GOARCH=amd64 -w /go/src/${PROJECT_NAME}/cmd/gosserver ${BUILD_CONTAINER} \
		go build -v -buildvcs=false
	mv ./gosserver $(BUILD)/bin/

clean:
	rm -f ./gosserver