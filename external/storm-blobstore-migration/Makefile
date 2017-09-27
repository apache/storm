# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PACKAGE_NAME=blobstore-migrator.tgz

VERSION=$(shell cat VERSION || mvn help:evaluate -Dexpression=project.version | grep -v '^\[')

all: $(PACKAGE_NAME)

$(PACKAGE_NAME) : VERSION target/blobstore-migrator-$(VERSION).jar
	-@rm -Rf blobstore-migrator $(PACKAGE_NAME)
	mkdir blobstore-migrator
	cp target/blobstore-migrator-$(VERSION).jar blobstore-migrator/
	cp listHDFS.sh listLocal.sh migrate.sh VERSION blobstore-migrator/
	tar -cvzf $(PACKAGE_NAME) blobstore-migrator
	rm -Rf blobstore-migrator

target/blobstore-migrator-$(VERSION).jar :
	mvn clean install

VERSION :
	echo $(VERSION) >VERSION
