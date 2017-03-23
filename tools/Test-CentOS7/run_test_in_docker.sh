#!/bin/bash
cp ../../pgglaskugel .
sudo docker build -t=pgglaskugelcentos7 .
sudo docker run -it pgglaskugelcentos7
