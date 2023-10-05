#!/usr/bin/env bash

build_images=$1
run_test_service_name=mdw

if [ "$build_images" == "true" ]; then
  echo "------------"
  echo "Build images"
  echo "------------"
  bash build-images.sh
fi

echo "----------------"
echo "Start containers"
echo "----------------"
docker-compose up -d

for i in {1..60}; do
  unhealthy_present="false"
  echo "-----------------------------------"
  echo "Check docker containers status: $i"
  echo "-----------------------------------"
  container_ids=$(docker-compose ps -q)
  for container_id in $container_ids
  do
    status=$(docker inspect $container_id --format "{{.State.Health.Status}}")
    if [ "$status" != "healthy" ]; then
      unhealthy_present="true"
      docker_name=$(docker container ls --all --no-trunc --filter "id=$container_id" --format "{{.Names}}")
      echo "Container '$docker_name' is not in a healthy status yet. Current status is '$status'."
    fi
  done
  if [ "$unhealthy_present" == "true" ]; then
    sleep 10
  else
    echo "---------------------------------------"
    echo "All containers are in the healthy state"
    echo "---------------------------------------"
    break;
  fi
done

if [ "$unhealthy_present" == "true" ]; then
    echo "--------------------------------------------"
    echo "Some containers are not in the healthy state"
    echo "--------------------------------------------"
    docker-compose ps
    exit 1
fi

echo "-------------------------"
echo "Start running smoke tests"``
echo "-------------------------"
docker-compose exec $run_test_service_name sudo -H -u gpadmin bash -l -c 'pushd $TEST_HOME && make GROUP=smoke'

echo "-------------------------------"
echo "Start running integration tests"
echo "-------------------------------"
docker-compose exec $run_test_service_name sudo -H -u gpadmin bash -l -c 'pushd $TEST_HOME && make GROUP=gpdb'

echo "--------------------"
echo "Start copy artifacts"
echo "--------------------"
mkdir -p artifacts
docker-compose cp $run_test_service_name:/home/gpadmin/workspace/pxf/automation/target/surefire-reports ./artifacts
docker-compose cp $run_test_service_name:/home/gpadmin/workspace/pxf/automation/tincrepo ./artifacts

echo "-------------------"
echo "Shutdown containers"
echo "-------------------"
docker-compose down
