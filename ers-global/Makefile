.phony: openshift embedded run clean

openshift:
	@echo Making Openshift version
	mvn package -Dmaven.test.skip=true -f pom.xml

embedded:
	@echo Making Embedded version
	mvn package -Pembedded -Dmaven.test.skip=true -f pom.xml

run:
	@bash target/bin/webapp start-embedded=yes

clean:
	@rm -rf target
