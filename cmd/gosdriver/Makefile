include ../../vars.make

all:
	sudo docker run --rm --name ${PROJECT_NAME}-compiler -v ${PROJECT_DIR}:/go/src/${PROJECT_NAME} -w /go/src/${PROJECT_NAME}/cmd/gosdriver ${BUILD_CONTAINER} go build -v
	mv ./gosdriver $(BUILD)/bin/

clean:
	rm -f ./gosdriver