## How to build PXF Docker image
From the root pxf folder run:
```bash
docker build --target test -t gpdb6_pxf_regress:latest -f arenadata/Dockerfile .
```
For default Docker will use image "hub.adsw.io/library/gpdb6_regress:adb-6.x-dev" (see ARG GPDB_IMAGE in the Dockerfile). It may be changed by the `--build-arg` param:
```bash
docker build --target test -t gpdb7_pxf_regress:latest --build-arg GPDB_IMAGE="hub.adsw.io/library/gpdb7_u22:latest" -f arenadata/Dockerfile .
```
This will build an image called `gpdb6_pxf_regress` with the tag `latest`. This image is based on `gpdb6_regress:latest`, which additionally contains pxf sources and pxf artifacts tarball in `/tmp/build/pxf_src` and `/tmp/build/pxf_tarball` folders respectively.

## How to test PXF
During the image building phase `compile_pxf.bash` script additionally calls `test` make target, which calls `make -C cli/go/src/pxf-cli test` and `make -C server test` commands.
To additionally test `fdw` and `external-table` parts you may call:
```bash
docker run --rm -it \
  --privileged --sysctl kernel.sem="500 1024000 200 4096" \
  gpdb6_pxf_regress:latest /tmp/build/pxf_src/arenadata/test_in_docker.sh
```
And the same for adb 7.x: 
```bash
docker run --rm -it \
  --privileged --sysctl kernel.sem="500 1024000 200 4096" \
  gpdb7_pxf_regress:latest /tmp/build/pxf_src/arenadata/test_in_docker.sh
```


## How to test PXF with TLS support

To test PXF with TLS we build PXF with Dockerfile which has PXF set up with SSL support:

For adb 6.x images:

```bash
docker build -t gpdb6_pxf_regress_ssl:latest -f arenadata/Dockerfile .
```

To additionally test `fdw` and `external-table` parts you may call:
```bash
docker run --rm -it \
  --privileged --sysctl kernel.sem="500 1024000 200 4096" \
  gpdb6_pxf_regress_ssl:latest /tmp/build/pxf_src/arenadata/test_in_docker.sh
```

For adb 7.x images:

```bash
docker build -t gpdb7_pxf_regress_ssl:latest --build-arg GPDB_IMAGE="hub.adsw.io/library/gpdb7_u22:latest" -f arenadata/Dockerfile .
```

To additionally test `fdw` and `external-table` parts you may call:
```bash
docker run --rm -it \
  --privileged --sysctl kernel.sem="500 1024000 200 4096" \
  gpdb7_pxf_regress_ssl:latest /tmp/build/pxf_src/arenadata/test_in_docker.sh
```
