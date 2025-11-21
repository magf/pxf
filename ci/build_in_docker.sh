#!/bin/bash
# Fot build in docker using developer's Greengage image
# image=greengagedb/ggdb6_ubuntu:6.29.1
# version=dh-6.29.1
# or
# image=ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest
# version=ghcr-latest

set -eu

export JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS:-'-Dfile.encoding=UTF8'}
export DEBIAN_FRONTEND=${DEBIAN_FRONTEND:-noninteractive}

export GOPATH=/opt/go
export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin

DEB=${DEB:-/deb/greengage.deb}

# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# "$SCRIPT_DIR/set_azure_sources_list.sh"

mime_type='application/vnd.debian.binary-package'

if [ ! -r "$DEB" ] ; then
  echo -n "GreengageDB deb-package file '$DEB' "
  echo "not exists or not readable. Use 'DEB=<ggdb_deb_file> $0' for proper file location"
  exit 1
elif [ "$(file -Lb --mime-type "$DEB")" != "$mime_type" ] ; then
  echo "not a $mime_type. Use 'DEB=<ggdb_deb_file> $0' for proper file location"
  exit 1
fi

pkgs='openjdk-17-jdk' # unzip vim nano ksh locales
echo -n "Installing $pkgs via apt... "
{
  apt-get -yq update
  apt-get -yq install --no-install-recommends $pkgs
  apt-get clean
} &>/dev/null ; echo "Done"

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
# mkdir -p "$GPHOME"
# tar -xzf "$DEV_HOME/bin_gpdb/bin_gpdb.tar.gz" -C "$GPHOME/"

apt-get -f install -y $DEB

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

# shellcheck source=/dev/null
# pg_config

make all install pkg-deb

# rm "$HOME/.cache" -rf
# rm "$HOME/.gitconfig" -rf
# rm "$HOME/.gradle" -rf

# rm -rf /var/lib/apt/lists/*
# rm -rf /tmp/*
