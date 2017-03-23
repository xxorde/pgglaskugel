FROM centos:7
MAINTAINER Alexander Sosna version: 0.1 

ENV container docker
#RUN yum -y install sudo
#RUN yum -y install wget
#RUN yum -y install lsof
#RUN yum -y install rng-tools
#RUN yum -y install https://download.postgresql.org/pub/repos/yum/$PG_VERSION/redhat/rhel-7-x86_64/pgdg-centos$PGVERSION-$PG_VERSION-3.noarch.rpm
#RUN yum -y install postgresql$PGVERSION
#RUN yum -y install postgresql$PGVERSION-server
#RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
#RUN yum -y install zstd

ADD ./pgglaskugel /usr/bin/pgglaskugel
ADD ./pgCentOS7.sh /usr/bin/pgCentOS7.sh
CMD ["/usr/bin/pgCentOS7.sh"] 
