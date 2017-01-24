#!/bin/bash
# Copyright © 2017 Alexander Sosna <alexander@xxor.de>
set -e -x
NAME=pgglaskugel
TAG=$(git tag)
DEST=$1
XZ=pgGlaskugel$TAG.tar.xz

go build -o $NAME
cp $NAME $DEST/
tar cfJ $XZ $NAME README.md docs LICENSE
cp $XZ $DEST/