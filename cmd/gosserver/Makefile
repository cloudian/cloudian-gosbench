include ../../vars.make

all:
	sudo docker run --rm --name ${PROJECT_NAME}-compiler -v ${PROJECT_DIR}:/go/src/${PROJECT_NAME} -w /go/src/${PROJECT_NAME}/cmd/gosserver ${BUILD_CONTAINER} go build -v
	mv ./gosserver $(BUILD)/bin/

clean:
	rm -f ./gosserver