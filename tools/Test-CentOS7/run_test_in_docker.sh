#!/bin/bash
if [ -f ../../pgglaskugel ]
  then
    cp ../../pgglaskugel .
  else
    echo "ERROR: Could not find pgglaskugel"
    exit 1
fi

echo "GPG takes longer when generating keys if you don't have enough entropy"
sudo docker build -t=pgglaskugelcentos7 .
sudo docker run -it pgglaskugelcentos7
rm pgglaskugel
