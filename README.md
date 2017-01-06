![logo](docs/img/glaskugel.png)

Disclaimer
==========
This is a personal work-in-progress project!
Do not expect anything to work as intended jet!

Feel free to send bug reports! :)

pgGlaskugel
===========
This should become an easy to use (backup) tool for PostgreSQL.

Design
======
The tool should have an easy to use CLI (like git / docker) and can also be automated easy as well.

Configuration
-------------
The configuration should be easy to use and manage.

Setup
-----
Comes with a simple self setup.

Achieve
-------
Can be used to achieve to:
* local storage / network mounts
* S3 / minio


Example architecture
====================

Distributed or Single Node
============================
![decentralized](docs/img/arch_overview_decentralized.png)

This is a distributed or single node approach.
On every database server there is also an instance of pgGlaskugel.

Local storage or network mounts are accessed through the local file system.
S3 compatible object storage can be accessed by pgGlaskugel directly.

## Backup
Backups are done by calling `pgGlaskugel basebackup`. This can happen by hand, cronjob or an automation tool like Ansible.

## WAL Archiving
If WAL Archiving should be used PostgreSQL's `archive_command` is set to `pgGlaskugel archive %p` so that PostgreSQL calls pgGlaskugel for every ready WAL file.

## Retention Policy
Retention policy is enforced by calling `pgGlaskugel cleanup --retain <NUMBER OF BACKUPS TO KEEP> --force-retain`.
This is normally done via cronjob on the same machine (but there are altho other methods).

## Restore Backup
Backups are restored by a local call to `pgGlaskugel  restore --backup <BACKUP NAME> --restore-to <PATH TO NEW INSTANCE>`


Centralized Backup Server
=========================
![decentralized](docs/img/arch_overview_centralized.png)

This example uses a central backup server that manages the backups of many database servers.

## Backups
Backups are called via cronjob or another tool according to schedule.
For every server to backup there is a different config file.
```yaml
pgGlaskugel --config server_to_backup.yml archive %p
```

Orchestration will maybe integrated later. 

## WAL Archiving
If WAL Archiving should be used PostgreSQL's `archive_command` is set to `pgGlaskugel archive %p` so that PostgreSQL calls pgGlaskugel for every ready WAL file.
Because WAL is pushed by PostgreSQL there are two requirements:
* pgGlaskugel on the database server
* Access to the WAL storage from database server


Usage
=====
