#!/bin/bash
# Copyright © 2017 Alexander Sosna <alexander@xxor.de>
set -e -x
NAME=pgglaskugel
DEST=$1
XZ=pgGlaskugel.tar.xz

go build -o $NAME
cp $NAME $DEST/
tar cfJ $XZ $NAME README.md docs LICENSE
cp $XZ $DEST/