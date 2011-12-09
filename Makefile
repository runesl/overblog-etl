CLASSPATH=lib/riak-client-1.0.2.jar:lib/httpclient-4.1.2.jar:lib/httpcore-4.1.2.jar:lib/log4j-log4j.jar

all:
	javac -d out/production -cp ${CLASSPATH} src/*
