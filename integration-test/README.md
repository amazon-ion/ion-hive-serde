# Integration Tests
Integration tests utilized a dockerized hive installation to run queries against docker locally. The tests can be 
run through `gradle test` or in your IDE.   

## Requirements 
The integration tests use docker to setup all system dependencies, e.g. hadoop, hive and postgres, so docker **must**
be installed. 

Official docker documentation: https://www.docker.com/get-started.

### Running tests in your IDE
The creation of test data and hive tables are all managed by the tests and test suite lifecycle, but they require the 
docker container to be up and running. The following tasks help with that:  
* `gradle testSetUp`: will prepare the test environment, including starting the docker container. 
* `gradle testTearDown`: stops the docker container and runs other clean up tasks. 
* `gradle tailHiveLog`: will tail hive logs, useful as not much useful information is propagated in the JDBC driver 
exception. The log is generated when needed, so you may need to run a test before running this task. 

