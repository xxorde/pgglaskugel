#!/bin/bash
# Copyright © 2017 Alexander Sosna <alexander@xxor.de>

# TODO process-description
set -e -x
NAME=pgglaskugel
DEST=$1
ARCHIVE_NAME=pgGlaskugel.tar.xz

if [ ! -d ${DEST} ]; then
	echo "creating ${DEST} directory"
	mkdir -p ${DEST}
fi

go build -o ${NAME}
cp ${NAME} ${DEST}/
tar cfJ ${ARCHIVE_NAME} ${NAME} README.md docs LICENSE
cp ${ARCHIVE_NAME} ${DEST}/
