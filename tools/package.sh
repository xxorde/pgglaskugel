#!/bin/bash
# Copyright © 2017 Alexander Sosna <alexander@xxor.de>
set -e -x
TAG=-$(git tag)
DEST=$1

go build
cp pgGlaskugel $DEST/
tar cfJ $DEST/pgGlaskugel$TAG.tar.xz  pgGlaskugel
