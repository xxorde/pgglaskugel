#!/bin/bash
# Copyright © 2017 Alexander Sosna <alexander@xxor.de>

# TODO process-description
set -e -x
NAME=pgglaskugel
DEST=$1
BUILD=build
BIN=${BUILD}/usr/bin
SHARE=${BUILD}/usr/share/pgglaskugel
ARCHIVE_NAME=pgGlaskugel.tar.xz

# Create DEST if needed
if [ ! -d ${DEST} ]; then
	echo "creating ${DEST} directory"
	mkdir -p ${DEST}
fi

# Cleanup BUILD
if [ -d ${BUILD} ]; then
	rm -rf ${BUILD}
fi

# Create folders
mkdir -p ${BIN}
mkdir -p ${SHARE}

# Build
go build -o ${NAME}

# Copy executeable
cp ${NAME} ${BIN}/

# Copy docs
cp -r README.md docs LICENSE ${SHARE}/

# Create archive
tar cfJ ${ARCHIVE_NAME} -C ${BUILD} .

# Copy artifacts
if [ ! -d ${DEST} ]; then
	cp ${ARCHIVE_NAME} ${DEST}/
fi
