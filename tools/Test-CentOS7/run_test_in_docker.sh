#!/bin/bash
set -e
if [ -f ../../pgglaskugel ]
  then
    cp ../../pgglaskugel .
  elif [ ! -f ./pgglaskugel ]
    then
      echo "ERROR: Could not find pgglaskugel"
      exit 1
fi

echo "GPG takes longer when generating keys if you don't have enough entropy"
sudo docker build -t=pgglaskugelcentos7 .

sudo docker run -it pgglaskugelcentos7 /usr/bin/pgCentOS7.sh file
sudo docker run -it pgglaskugelcentos7 /usr/bin/pgCentOS7.sh s3
sudo docker run -it pgglaskugelcentos7 /usr/bin/pgCentOS7.sh minio
sudo docker run -it pgglaskugelcentos7 /usr/bin/pgCentOS7.sh minioC
