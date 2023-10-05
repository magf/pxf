### How to run integration tests

Integration tests start from the docker container with a service name `mdw`

## Run all integration tests

```shell
bash run_it.sh true
```
The script builds images, starts the test environment and runs integration tests.

## Run specific integration tests 

Build images:
```shell
bash build-images.sh
```
Start test environment:
```shell
docker-compose up -d
```
Wait until all containers started and their status is healthy.

It is possible to run a specific test, all tests from a specific class or group.

Start all tests from the specific class:
```shell
docker-compose exec mdw sudo -H -u gpadmin bash -l -c 'pushd $TEST_HOME && make TEST=JdbcHiveTest'
```

Start a specific test:
```shell
docker-compose exec mdw sudo -H -u gpadmin bash -l -c 'pushd $TEST_HOME && make TEST=JdbcHiveTest#jdbcHiveRead'
```

Start all tests from the specific group:
```shell
docker-compose exec mdw sudo -H -u gpadmin bash -l -c 'pushd $TEST_HOME && make GROUP=gpdb'
```
