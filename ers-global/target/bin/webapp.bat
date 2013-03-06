@REM ----------------------------------------------------------------------------
@REM Copyright 2001-2004 The Apache Software Foundation.
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License");
@REM you may not use this file except in compliance with the License.
@REM You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.
@REM ----------------------------------------------------------------------------
@REM

@echo off

set ERROR_CODE=0

:init
@REM Decide how to startup depending on the version of windows

@REM -- Win98ME
if NOT "%OS%"=="Windows_NT" goto Win9xArg

@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" @setlocal

@REM -- 4NT shell
if "%eval[2+2]" == "4" goto 4NTArgs

@REM -- Regular WinNT shell
set CMD_LINE_ARGS=%*
goto WinNTGetScriptDir

@REM The 4NT Shell from jp software
:4NTArgs
set CMD_LINE_ARGS=%$
goto WinNTGetScriptDir

:Win9xArg
@REM Slurp the command line arguments.  This loop allows for an unlimited number
@REM of arguments (up to the command line limit, anyway).
set CMD_LINE_ARGS=
:Win9xApp
if %1a==a goto Win9xGetScriptDir
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto Win9xApp

:Win9xGetScriptDir
set SAVEDIR=%CD%
%0\
cd %0\..\.. 
set BASEDIR=%CD%
cd %SAVEDIR%
set SAVE_DIR=
goto repoSetup

:WinNTGetScriptDir
set BASEDIR=%~dp0\..

:repoSetup


if "%JAVACMD%"=="" set JAVACMD=java

if "%REPO%"=="" set REPO=%BASEDIR%\repo

set CLASSPATH="%BASEDIR%"\etc;"%REPO%"\org\apache\tomcat\embed\tomcat-embed-core\7.0.34\tomcat-embed-core-7.0.34.jar;"%REPO%"\org\apache\tomcat\embed\tomcat-embed-logging-juli\7.0.34\tomcat-embed-logging-juli-7.0.34.jar;"%REPO%"\org\apache\tomcat\embed\tomcat-embed-jasper\7.0.34\tomcat-embed-jasper-7.0.34.jar;"%REPO%"\org\eclipse\jdt\core\compiler\ecj\3.7.2\ecj-3.7.2.jar;"%REPO%"\org\apache\tomcat\tomcat-jasper\7.0.34\tomcat-jasper-7.0.34.jar;"%REPO%"\org\apache\tomcat\tomcat-servlet-api\7.0.34\tomcat-servlet-api-7.0.34.jar;"%REPO%"\org\apache\tomcat\tomcat-juli\7.0.34\tomcat-juli-7.0.34.jar;"%REPO%"\org\apache\tomcat\tomcat-jsp-api\7.0.34\tomcat-jsp-api-7.0.34.jar;"%REPO%"\org\apache\tomcat\tomcat-el-api\7.0.34\tomcat-el-api-7.0.34.jar;"%REPO%"\org\apache\tomcat\tomcat-jasper-el\7.0.34\tomcat-jasper-el-7.0.34.jar;"%REPO%"\org\apache\tomcat\tomcat-api\7.0.34\tomcat-api-7.0.34.jar;"%REPO%"\org\apache\tomcat\tomcat-util\7.0.34\tomcat-util-7.0.34.jar;"%REPO%"\io\cassandra\sdk\cassandraio-java\0.0.1\cassandraio-java-0.0.1.jar;"%REPO%"\com\google\code\gson\gson\2.0\gson-2.0.jar;"%REPO%"\org\apache\cassandra\cassandra-clientutil\1.1.10\cassandra-clientutil-1.1.10.jar;"%REPO%"\com\google\guava\guava\r08\guava-r08.jar;"%REPO%"\org\apache\cassandra\cassandra-thrift\1.1.10\cassandra-thrift-1.1.10.jar;"%REPO%"\commons-lang\commons-lang\2.4\commons-lang-2.4.jar;"%REPO%"\org\slf4j\slf4j-api\1.6.4\slf4j-api-1.6.4.jar;"%REPO%"\org\apache\thrift\libthrift\0.7.0\libthrift-0.7.0.jar;"%REPO%"\javax\servlet\servlet-api\2.5\servlet-api-2.5.jar;"%REPO%"\org\apache\httpcomponents\httpclient\4.0.1\httpclient-4.0.1.jar;"%REPO%"\org\apache\httpcomponents\httpcore\4.0.1\httpcore-4.0.1.jar;"%REPO%"\commons-logging\commons-logging\1.1.1\commons-logging-1.1.1.jar;"%REPO%"\commons-codec\commons-codec\1.4\commons-codec-1.4.jar;"%REPO%"\commons-cli\commons-cli\1.1\commons-cli-1.1.jar;"%REPO%"\org\hectorclient\hector-core\1.1-1\hector-core-1.1-1.jar;"%REPO%"\commons-pool\commons-pool\1.5.3\commons-pool-1.5.3.jar;"%REPO%"\org\apache\cassandra\cassandra-all\1.1.0\cassandra-all-1.1.0.jar;"%REPO%"\org\xerial\snappy\snappy-java\1.0.4.1\snappy-java-1.0.4.1.jar;"%REPO%"\com\ning\compress-lzf\0.8.4\compress-lzf-0.8.4.jar;"%REPO%"\com\googlecode\concurrentlinkedhashmap\concurrentlinkedhashmap-lru\1.2\concurrentlinkedhashmap-lru-1.2.jar;"%REPO%"\org\antlr\antlr\3.2\antlr-3.2.jar;"%REPO%"\org\antlr\antlr-runtime\3.2\antlr-runtime-3.2.jar;"%REPO%"\org\antlr\stringtemplate\3.2\stringtemplate-3.2.jar;"%REPO%"\antlr\antlr\2.7.7\antlr-2.7.7.jar;"%REPO%"\org\apache\cassandra\deps\avro\1.4.0-cassandra-1\avro-1.4.0-cassandra-1.jar;"%REPO%"\org\codehaus\jackson\jackson-mapper-asl\1.4.0\jackson-mapper-asl-1.4.0.jar;"%REPO%"\org\codehaus\jackson\jackson-core-asl\1.4.0\jackson-core-asl-1.4.0.jar;"%REPO%"\org\mortbay\jetty\jetty\6.1.22\jetty-6.1.22.jar;"%REPO%"\org\mortbay\jetty\jetty-util\6.1.22\jetty-util-6.1.22.jar;"%REPO%"\org\mortbay\jetty\servlet-api\2.5-20081211\servlet-api-2.5-20081211.jar;"%REPO%"\jline\jline\0.9.94\jline-0.9.94.jar;"%REPO%"\com\googlecode\json-simple\json-simple\1.1\json-simple-1.1.jar;"%REPO%"\com\github\stephenc\high-scale-lib\high-scale-lib\1.1.2\high-scale-lib-1.1.2.jar;"%REPO%"\org\yaml\snakeyaml\1.9\snakeyaml-1.9.jar;"%REPO%"\edu\stanford\ppl\snaptree\0.1\snaptree-0.1.jar;"%REPO%"\com\yammer\metrics\metrics-core\2.0.3\metrics-core-2.0.3.jar;"%REPO%"\log4j\log4j\1.2.16\log4j-1.2.16.jar;"%REPO%"\org\slf4j\slf4j-log4j12\1.6.1\slf4j-log4j12-1.6.1.jar;"%REPO%"\com\github\stephenc\eaio-uuid\uuid\3.2.0\uuid-3.2.0.jar;"%REPO%"\com\ecyrd\speed4j\speed4j\0.9\speed4j-0.9.jar;"%REPO%"\org\semanticweb\yars\nxparser\1.2.2\nxparser-1.2.2.jar;"%REPO%"\org\htmlparser\htmlparser\2.1\htmlparser-2.1.jar;"%REPO%"\org\htmlparser\htmllexer\2.1\htmllexer-2.1.jar;"%REPO%"\org\junit\com.springsource.org.junit\4.9.0\com.springsource.org.junit-4.9.0.jar;"%REPO%"\org\openrdf\sesame\sesame-model\2.6.2\sesame-model-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-util\2.6.2\sesame-util-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-rio-api\2.6.2\sesame-rio-api-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-rio-binary\2.6.2\sesame-rio-binary-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-rio-ntriples\2.6.2\sesame-rio-ntriples-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-rio-n3\2.6.2\sesame-rio-n3-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-rio-turtle\2.6.2\sesame-rio-turtle-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-rio-rdfxml\2.6.2\sesame-rio-rdfxml-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-rio-trix\2.6.2\sesame-rio-trix-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-rio-trig\2.6.2\sesame-rio-trig-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-query\2.6.2\sesame-query-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-queryalgebra-model\2.6.2\sesame-queryalgebra-model-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-queryalgebra-evaluation\2.6.2\sesame-queryalgebra-evaluation-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-repository-sparql\2.6.2\sesame-repository-sparql-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-repository-api\2.6.2\sesame-repository-api-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-http-client\2.6.2\sesame-http-client-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-http-protocol\2.6.2\sesame-http-protocol-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-queryresultio-api\2.6.2\sesame-queryresultio-api-2.6.2.jar;"%REPO%"\commons-httpclient\commons-httpclient\3.1\commons-httpclient-3.1.jar;"%REPO%"\org\openrdf\sesame\sesame-queryresultio-sparqlxml\2.6.2\sesame-queryresultio-sparqlxml-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-queryparser-api\2.6.2\sesame-queryparser-api-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-queryparser-serql\2.6.2\sesame-queryparser-serql-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-queryparser-sparql\2.6.2\sesame-queryparser-sparql-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-queryresultio-binary\2.6.2\sesame-queryresultio-binary-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-queryresultio-sparqljson\2.6.2\sesame-queryresultio-sparqljson-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-queryresultio-text\2.6.2\sesame-queryresultio-text-2.6.2.jar;"%REPO%"\net\sf\opencsv\opencsv\2.0\opencsv-2.0.jar;"%REPO%"\org\openrdf\sesame\sesame-repository-manager\2.6.2\sesame-repository-manager-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-repository-event\2.6.2\sesame-repository-event-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-repository-sail\2.6.2\sesame-repository-sail-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-sail-api\2.6.2\sesame-sail-api-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-sail-memory\2.6.2\sesame-sail-memory-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-sail-inferencer\2.6.2\sesame-sail-inferencer-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-repository-http\2.6.2\sesame-repository-http-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-repository-dataset\2.6.2\sesame-repository-dataset-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-repository-contextaware\2.6.2\sesame-repository-contextaware-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-sail-nativerdf\2.6.2\sesame-sail-nativerdf-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-sail-rdbms\2.6.2\sesame-sail-rdbms-2.6.2.jar;"%REPO%"\commons-dbcp\commons-dbcp\1.3\commons-dbcp-1.3.jar;"%REPO%"\org\openrdf\sesame\sesame-config\2.6.2\sesame-config-2.6.2.jar;"%REPO%"\ch\qos\logback\logback-core\0.9.28\logback-core-0.9.28.jar;"%REPO%"\ch\qos\logback\logback-classic\0.9.28\logback-classic-0.9.28.jar;"%REPO%"\org\openrdf\sesame\sesame-runtime\2.6.2\sesame-runtime-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-console\2.6.2\sesame-console-2.6.2.jar;"%REPO%"\org\openrdf\sesame\sesame-http-server-spring\2.6.2\sesame-http-server-spring-2.6.2.jar;"%REPO%"\javax\servlet\jstl\1.1.2\jstl-1.1.2.jar;"%REPO%"\taglibs\standard\1.1.2\standard-1.1.2.jar;"%REPO%"\org\springframework\spring-aop\2.5.6\spring-aop-2.5.6.jar;"%REPO%"\aopalliance\aopalliance\1.0\aopalliance-1.0.jar;"%REPO%"\org\springframework\spring-beans\2.5.6\spring-beans-2.5.6.jar;"%REPO%"\org\springframework\spring-core\2.5.6\spring-core-2.5.6.jar;"%REPO%"\org\springframework\spring-webmvc\2.5.6\spring-webmvc-2.5.6.jar;"%REPO%"\org\springframework\spring-context\2.5.6\spring-context-2.5.6.jar;"%REPO%"\org\springframework\spring-context-support\2.5.6\spring-context-support-2.5.6.jar;"%REPO%"\org\springframework\spring-web\2.5.6\spring-web-2.5.6.jar;"%REPO%"\cglib\cglib\2.2\cglib-2.2.jar;"%REPO%"\asm\asm\3.1\asm-3.1.jar;"%REPO%"\tomcat\servlet-api\5.5.23\servlet-api-5.5.23.jar;"%REPO%"\org\slf4j\slf4j-simple\1.6.4\slf4j-simple-1.6.4.jar;"%REPO%"\ch\unifr\diuf\ers-global-server\1.1-SNAPSHOT\ers-global-server-1.1-SNAPSHOT.jar
set EXTRA_JVM_ARGUMENTS=
goto endInit

@REM Reaching here means variables are defined and arguments have been captured
:endInit

%JAVACMD% %JAVA_OPTS% %EXTRA_JVM_ARGUMENTS% -classpath %CLASSPATH_PREFIX%;%CLASSPATH% -Dapp.name="webapp" -Dapp.repo="%REPO%" -Dbasedir="%BASEDIR%" launch.Main %CMD_LINE_ARGS%
if ERRORLEVEL 1 goto error
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=1

:end
@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" goto endNT

@REM For old DOS remove the set variables from ENV - we assume they were not set
@REM before we started - at least we don't leave any baggage around
set CMD_LINE_ARGS=
goto postExec

:endNT
@endlocal

:postExec

if "%FORCE_EXIT_ON_ERROR%" == "on" (
  if %ERROR_CODE% NEQ 0 exit %ERROR_CODE%
)

exit /B %ERROR_CODE%
