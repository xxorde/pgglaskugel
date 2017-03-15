#!/bin/bash


# Copyright © 2017 Hendrik Siewert <hendrik.siewert@credativ.de>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.


# check distro and version
checkdistroversion(){
#echo "$(cat /etc/os-release | grep ID | head -1 | cut -d"=" -f2)"
var=$(cat /etc/os-release | grep ID | head -1 | cut -d"=" -f2 | grep centos)

var2=$(cat /etc/os-release | grep VERSION_ID | cut -d"\"" -f2)
#echo "$(cat /etc/os-release | grep VERSION_ID | cut -d"\"" -f2)"

if [[ ! -z "$var" ]]
  then
    if [[ ! $var2 =~ ^7.* ]]
      then
        echo "Nicht Version 7 von CentOS"
        exit 1
    fi
  else
    echo "Kein CentOS 7 installiert"
    exit 1
fi
echo "Seems like a CentOS7 version..."

}

minioinstall(){
echo "Getting Minio..."
wget https://dl.minio.io/server/minio/release/linux-amd64/minio
chmod 755 minio
mv ./minio /usr/bin
cat > /lib/systemd/system/minio.service << EOL
[Unit]
Description=Minio Server
#depends on Network

[Service]
Type=simple
Environment="IP=127.0.0.1" "PORT=9000" "DATADIR=/var/lib/minio"
ExecStart=/usr/bin/minio server --address \${IP}:\${PORT} \${DATADIR}

[Install]
WantedBy=multi-user.target
EOL
systemctl daemon-reload
}
miniostart(){
echo "Starting Minio Server..."
#minio server --address 127.0.0.1:9000 /var/lib/minio > /home/vagrant/test &
systemctl start minio.service
sleep 3
}
miniogetkeys(){
echo "Looking for Minio keys..."
if [ ! -f /root/.minio/config.json ]
  then
    echo "config.json missing in /root/.minio... :/"
    exit 1
fi
accesskey=$(cat /root/.minio/config.json | grep accessKey | cut -d":" -f2 | cut -d"\"" -f2)
secretkey=$(cat /root/.minio/config.json | grep secretKey | cut -d":" -f2 | cut -d"\"" -f2)
echo "ACCESS KEY:$accesskey"
echo "SECRET KEY:$secretkey"
}

miniocheck(){

if [ ! -f /usr/bin/minio ] && [ ! -d /var/lib/minio ]
  then
    minioinstall
    miniostart
    miniogetkeys
  elif [ "active" == $(systemctl is-active minio.service) ]
    then
      miniogetkeys
  else
    echo "Installing/starting Minio went wrong!"
    exit 1
fi
}
testingnoenc(){

if [ -z "$(zstdcat $enctest)" ]
  then
    echo "Can't read basebackup..."
    exit 1
  elif [ -z "$(zstdcat $walenctest)" ]
    then
      echo "Can't read wal files..."
      exit 1
  else
    echo "Successfully read archived files!"
fi
}

# test encryption 
encrypttest(){
if [ "$1" == "file" ]
  then
    enctest=/var/lib/postgresql/backup/pgglaskugel/basebackup/$(ls /var/lib/postgresql/backup/pgglaskugel/basebackup)
    walenctest=/var/lib/postgresql/backup/pgglaskugel/wal/$(ls /var/lib/postgresql/backup/pgglaskugel/wal | head -1)
  elif [ "$1" == "s3" ]
    then
      enctest=/var/lib/minio/pgglaskugel-basebackup/$(ls /var/lib/minio/pgglaskugel-basebackup)
      walenctest=/var/lib/minio/pgglaskugel-wal/$(ls /var/lib/minio/pgglaskugel-wal | head -1)
  else
    echo "ERROR: Encryption test parameters failed"
    exit 1
fi
if [ -z "$enctest" ] || [ -z "$walenctest" ]
  then
    echo "Can't find archived files..."
    exit 1
fi
if [ ! -z "$2" ]
  then
    if [ "$2" == "noenc" ]
      then
        testingnoenc
      elif [ "$2" == "enc" ]
        then
          testingenc
      else
        echo "WRONG PARAMETERS in function: encrypttest"
        exit 1
    fi
  else
    echo "SECOND PARAMETER ISN'T GIVEN in function: encrypttest"
    exit 1
fi    
}
testingenc(){
if [ $(zstd -d $enctest > /dev/null 2>&1) ]
  then
    echo "BASEBACKUP DID NOT GET ENCRYPTED!!!"
    exit 1
  elif [ $(zstd -d $walenctest > /dev/null 2>&1) ]
    then
      echo "WAL FILES DID NOT GET ENCRYPTED"
      exit 1
  else
    echo "Successfully encrypted!"
fi
}

# clean directories if necessary
cleandirs(){
if [ ! -z "ls /var/lib/pgsql/9.5/data" ]
  then
    systemctl stop postgresql-9.5.service
    rm -rf /var/lib/pgsql/9.5/data/*   
    echo "Cleaning data dir..."
fi
if [ ! -z "ls /var/lib/postgresql/backup/pgglaskugel/basebackup/" ]
  then
    rm -rf /var/lib/postgresql/backup/pgglaskugel/basebackup/*
    rm -rf /var/lib/postgresql/backup/pgglaskugel/wal/*
    echo "Cleaning pgGlaskugel basebackup..."
fi
if [ ! -z "ls /var/lib/minio" ]
  then
    rm -rf /var/lib/minio/*
    echo "Cleaning Minio folders..."
fi
}

# move pgGlaskugel to /usr/bin
pathglaskugel(){
if [ -f "$1" ]
  then
    mv $1 /usr/bin
    echo "pgGlaskugel successfully moved to /usr/bin/"
elif [ -d "$1" ]
  then
    mv $1/pgglaskugel /usr/bin
    echo "pgGlaskugel successfully moved to /usr/bin/"
  else
    echo "Path is wrong"
    exit 1
fi
}

gpgcheck(){

#get public key id: gpg -k | grep pub | sed 1d | cut -d"/" -f 2 | cut -d" " -f1 
if [ -z "$(sudo -u postgres gpg -k | grep pub | sed 1d | cut -d"/" -f 2 | cut -d" " -f1)" ]
  then
    createKeyPair
fi
}

createKeyPair(){
rngd -r /dev/urandom
cat > /var/lib/pgsql/foo << EOL
%echo Generating a default key
Key-Type: default
Subkey-Type: default
Name-Real: Hen Tester
Name-Comment: with stupid passphrase
Name-Email: hen@foo.bar
%no-protection
%no-ask-passphrase
Expire-Date: 0
%commit
%echo done
EOL
chown postgres /var/lib/pgsql/foo
sudo -u postgres gpg --batch --gen-key /var/lib/pgsql/foo
}

Init (){
systemctl start postgresql-9.5.service
echo "Editing pg_hba.conf..."
cat > /var/lib/pgsql/9.5/data/pg_hba.conf << EOL
host    all             all             127.0.0.1/32            md5
local   all             postgres                                ident
local   replication     postgres                                ident
host    replication     postgres        127.0.0.1/32            md5
EOL

chown -R postgres /var/lib/pgsql/

echo "Set postgres password to postgres..."
sudo -u postgres psql -c "alter user postgres with password 'postgres';"
# Hier wird die pg_hba.conf zum ersten mal geladen
echo "Reloading the pg_hba.conf..."
sudo -u postgres psql -c "select pg_reload_conf();"
}

s3config(){

echo "Create pgGlaskugel S3 config..."
mkdir /var/lib/pgsql/.pgglaskugel
if [ -f /var/lib/pgsql/.pgglaskugel/config.yml ]
  then
    rm /var/lib/pgsql/.pgglaskugel/config.yml
fi
cat > /var/lib/pgsql/.pgglaskugel/config.yml << EOL
---
encrypt: true
debug: true
recipient: hen@foo.bar
archive_to: s3
backup_to: s3
s3_access_key: $accesskey
s3_secret_key: $secretkey
s3_ssl: false
EOL
}
s3confignoenc(){

echo "Create pgGlaskugel S3 config..."
mkdir /var/lib/pgsql/.pgglaskugel
if [ -f /var/lib/pgsql/.pgglaskugel/config.yml ]
  then
    rm /var/lib/pgsql/.pgglaskugel/config.yml
fi
cat > /var/lib/pgsql/.pgglaskugel/config.yml << EOL
---
encrypt: false
debug: true
archive_to: s3
backup_to: s3
s3_access_key: $accesskey
s3_secret_key: $secretkey
s3_ssl: false
EOL
}

fileconfig(){

echo "Create pgGlaskugel file config..."
mkdir /var/lib/pgsql/.pgglaskugel
if [ -f /var/lib/pgsql/.pgglaskugel/config.yml ]
  then
    rm /var/lib/pgsql/.pgglaskugel/config.yml
fi
cat > /var/lib/pgsql/.pgglaskugel/config.yml << EOL
---
encrypt: true
debug: true
recipient: hen@foo.bar
EOL
}
fileconfignoenc(){

echo "Create pgGlaskugel file config..."
mkdir /var/lib/pgsql/.pgglaskugel
if [ -f /var/lib/pgsql/.pgglaskugel/config.yml ]
  then
    rm /var/lib/pgsql/.pgglaskugel/config.yml
fi
cat > /var/lib/pgsql/.pgglaskugel/config.yml << EOL
---
encrypt: false 
debug: true
EOL
}

pgglaskugelsetup(){
##################################Fix##################################
mkdir -p /var/lib/postgresql/backup/pgglaskugel/basebackup
chown -R postgres /var/lib/postgresql/backup
#######################################################################

echo "Starting pgGlaskugel setup..."
sudo -u postgres pgglaskugel setup
systemctl restart postgresql-9.5.service 
}

pgglaskugelbasebackup(){

#first table
sudo -u postgres psql -c "create table test0 (num int, Primary Key(num));"

#second table
sudo -u postgres psql -c "create table test1 (num int, Primary Key(num));"

echo "Creating basebackup"
sudo -u postgres pgglaskugel basebackup

#third table
sudo -u postgres psql -c "create table test2 (num int, Primary Key(num));"
#switch_xlog
sudo -u postgres psql -c "SELECT pg_switch_xlog();"

#save tables in var
Test1=$(sudo -u postgres psql -c "\dt")
}
installpackages(){
yum -y install wget
yum -y install rng-tools
yum -y install https://download.postgresql.org/pub/repos/yum/9.5/redhat/rhel-7-x86_64/pgdg-centos95-9.5-3.noarch.rpm
yum -y install postgresql95
yum -y install postgresql95-server
yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
yum -y install zstd
}
checksu(){
if [ $EUID -ne 0 ]
  then
    echo "This script must be run as root"
    exit 1
fi
}

pgglaskugelrestore(){
echo "restoring backup..."
if [ "$1" == "file" ]
  then
    backupfilezst=$(ls /var/lib/postgresql/backup/pgglaskugel/basebackup)
  elif [ "$1" == "s3" ]
    then
      backupfilezst=$(ls /var/lib/minio/pgglaskugel-basebackup)
  else
    echo "ERROR: No Backupdirectory specified"
    exit 1
fi
if [[ -z $backupfilezst ]]
  then
    echo "No backup file found!"
    exit 1
  else
    echo "backup file $backupfilezst found"
fi
backupfilezst=$(basename $backupfilezst .zst)
sudo -u postgres pgglaskugel restore $backupfilezst /var/lib/pgsql/9.5/data
}
testingtables(){
## Compare postgres tables. save \dt in variables and compare
Test2=$(sudo -u postgres psql -c "\dt")
echo "TESTING NOW..."
if [ "$Test1" == "$Test2" ] && [[ $Test1 =~ .*test0.*test1.*test2.* ]] && [[ $Test2 =~ .*test0.*test1.*test2.* ]] 
  then
    echo "THIS TEST WAS SUCCESSFUL!"
  else
    echo "Database tables don't match! :("
    exit 1
fi
}
runs3enctest(){
cleandirs
/usr/pgsql-9.5/bin/postgresql95-setup initdb
Init
miniocheck
gpgcheck
s3config
pgglaskugelsetup
pgglaskugelbasebackup
echo "Testing encryption..."
encrypttest s3 enc
echo "Dropping old cluster..."
systemctl stop postgresql-9.5.service 
rm -rf /var/lib/pgsql/9.5/data/*
pgglaskugelrestore s3
echo "Configuring new cluster..."
Init
echo "Testing encryption again..."
encrypttest s3 enc
testingtables
}
runs3noenctest(){
cleandirs
/usr/pgsql-9.5/bin/postgresql95-setup initdb
Init
miniocheck
s3confignoenc
pgglaskugelsetup
pgglaskugelbasebackup
echo "Testing if backup is readable..."
encrypttest s3 noenc
echo "Dropping old cluster..."
systemctl stop postgresql-9.5.service
rm -rf /var/lib/pgsql/9.5/data/*
pgglaskugelrestore s3
echo "Configuring new cluster..."
Init
echo "Testing again if backup is readable..."
encrypttest s3 noenc
testingtables
}
runfileenctest(){
cleandirs
/usr/pgsql-9.5/bin/postgresql95-setup initdb
Init
gpgcheck
fileconfig
pgglaskugelsetup
pgglaskugelbasebackup
echo "Testing encryption..."
encrypttest file enc
echo "Dropping old cluster..."
systemctl stop postgresql-9.5.service 
rm -rf /var/lib/pgsql/9.5/data/*
pgglaskugelrestore file
echo "Configuring new cluster..."
Init
echo "Testing encryption again..."
encrypttest file enc
testingtables
}
runfilenoenctest(){
cleandirs
/usr/pgsql-9.5/bin/postgresql95-setup initdb
Init
fileconfignoenc
pgglaskugelsetup
pgglaskugelbasebackup
echo "Testing if backup is readable..."
encrypttest file noenc
echo "Dropping old cluster..."
systemctl stop postgresql-9.5.service
rm -rf /var/lib/pgsql/9.5/data/*
pgglaskugelrestore file
echo "Configuring new cluster..."
Init
echo "Testing again if back is readable..."
encrypttest file noenc
testingtables
}
#######################################################################
###############################START###################################
#######################################################################
# Check arguments
if [ $# -lt 1 ] || [ $# -gt  1 ]
  then
    if [ ! -f /usr/bin/pgglaskugel ]
      then
        echo "Usage: $0 <Path to pgGlaskugel>"
        exit 1
    fi
  elif [ ! -f /usr/bin/pgglaskugel ]
    then
      pathglaskugel $1
fi
checksu
echo "Check if CentOS7..."
checkdistroversion
installpackages

echo "#RUNNING S3 TEST WITH ENCRYPTION#"
runs3enctest
echo "#RUNNING S3 TEST WITHOUT ENCRYPTION#"
runs3noenctest
echo "#RUNNING FILE TEST WITH ENCRYPTION#"
runfileenctest
echo "#RUNNING FILE TEST WITHOUT ENCRYPTION#"
runfilenoenctest
echo "#ALL TESTS WERE SUCCESSFUL#"
exit 0
