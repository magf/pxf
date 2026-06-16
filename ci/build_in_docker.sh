#!/bin/bash
# ci/build_in_docker.sh
#
# Fot build in docker using stable Greengage images:
# image=greengagedb/ggdb6_ubuntu:6.31.0
# image=greengagedb/ggdb6_ubuntu24:6.31.0
# image=greengagedb/ggdb7_ubuntu:7.4.1
#
# or developer Greengage images:
# image=ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest
# image=ghcr.io/greengagedb/greengage/ggdb7_ubuntu:latest

# shellcheck disable=SC2086

set -eux

export GREENGAGE_VERSION=${GREENGAGE_VERSION:-6}

export JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS:-'-Dfile.encoding=UTF8'}
export DEBIAN_FRONTEND=${DEBIAN_FRONTEND:-noninteractive}

export GOPATH=/opt/go
export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin

export GREENGAGE_PACKAGE=${GREENGAGE_PACKAGE:-greengage$GREENGAGE_VERSION}
export GREENGAGE_REPO_URL=${GREENGAGE_REPO_URL:-'https://greengagedb.org'}

# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# "$SCRIPT_DIR/set_azure_sources_list.sh"

update-locale LANG=en_US.UTF-8

go_version=$(grep -E '^go [0-9]+\.[0-9]+\.[0-9]+' cli/go.mod | cut -d' ' -f2)
installed_go_version=$(go version 2>/dev/null | grep -Eo 'go[0-9]+\.[0-9]+\.[0-9]+' | tr -d 'go')

echo -n "Golang version $go_version "
if [ "$go_version" != "$installed_go_version" ] ; then
  echo "not found"
  if [ -n "$installed_go_version" ] ; then
    echo "Found Golang $installed_go_version. Removing"
    rm -rf /usr/local/go
  fi
  echo -n "Downloading and installing Golang $go_version... "
  {
    curl -L https://go.dev/dl/go$go_version.linux-amd64.tar.gz -o /tmp/go.tar.gz
    tar  -C /usr/local -xzf /tmp/go.tar.gz
    rm   /tmp/go.tar.gz
  } &>/dev/null ; echo 'Done'
else
  echo "found"
fi

git config --global --add safe.directory "$(pwd)"
localedef -c -i ru_RU -f CP1251 ru_RU.CP1251

# Install Greengage package
# shellcheck disable=SC1091 # External source
source /etc/os-release
echo "deb [signed-by=/etc/apt/keyrings/greengagedb.gpg] \
  ${GREENGAGE_REPO_URL}/repositories/ubuntu/${VERSION_ID}/x86_64 greengagedb main" \
  > /etc/apt/sources.list.d/greengagedb.list

pkgs="openjdk-17-jdk debhelper devscripts dh-python file $GREENGAGE_PACKAGE" # unzip vim nano ksh locales
echo -n "Installing $pkgs via apt... "

  apt-get -yq update
  apt-get -yq install --no-install-recommends $pkgs
  apt-get clean

known_locations='/opt /usr/lib'
GPHOME=$(dev/detect_gphome.bash "$known_locations")
if [ -z "$GPHOME" ] ; then
  echo "Greengage not found at known locations: '$known_locations'. Exiting"
  exit 1
else
  export GPHOME
  echo "Greengage found at $GPHOME"
fi

# GreengageDB environment variables
PATH="$GPHOME/bin:$PATH"
PYTHONPATH="$GPHOME/lib/python"
LD_LIBRARY_PATH="$GPHOME/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

if [ -e "$GPHOME/etc/openssl.cnf" ]; then
	OPENSSL_CONF="$GPHOME/etc/openssl.cnf"
fi

export PATH
export PYTHONPATH
export LD_LIBRARY_PATH
export OPENSSL_CONF

make all install pkg-deb
