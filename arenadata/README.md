## How to build PXF Docker image
From the root pxf folder run:
```bash
docker build -t gpdb6_pxf_regress:latest -f arenadata/Dockerfile .
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
