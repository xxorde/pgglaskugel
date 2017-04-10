#!/bin/bash
# Copyright © 2017 Alexander Sosna <alexander@xxor.de>

# TODO process-description
set -e -x
NAME=pgglaskugel
DEST=$1
ARCHIVE_NAME=pgGlaskugel.tar.xz

go get
make ${NAME}
make tarball
make test

if [ ! -v ${DEST} ]; then
        echo "DEST is set to ${DEST}, copy artifacts" 
        # Create DEST if needed
        if [ ! -d ${DEST} ]; then
                echo "Creating ${DEST} directory"
                mkdir -p ${DEST}
        fi
        # Copy artifacts
        if [ -d ${DEST} ]; then
                cp ${NAME} ${DEST}/
                cp ${ARCHIVE_NAME} ${DEST}/
        fi
fi

