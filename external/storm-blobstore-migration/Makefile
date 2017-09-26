PACKAGE_NAME=blobstore-migrator.tgz

VERSION=$(shell cat VERSION || mvn help:evaluate -Dexpression=project.version | grep -v '^\[')

all: $(PACKAGE_NAME)

$(PACKAGE_NAME) : VERSION target/blobstore-migrator-$(VERSION).jar
	-@rm -Rf blobstore-migrator $(PACKAGE_NAME)
	mkdir blobstore-migrator
	cp target/blobstore-migrator-$(VERSION).jar blobstore-migrator/
	cp listHDFS.sh listLocal.sh migrate.sh VERSION blobstore-migrator/
	tar -cvzf $(PACKAGE_NAME) blobstore-migrator
#	rm -Rf blobstore-migrator

target/blobstore-migrator-$(VERSION).jar :
	mvn clean install

VERSION :
	echo $(VERSION) >VERSION
