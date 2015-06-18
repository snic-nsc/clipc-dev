INSTALLATION
============

1. sqlite3

   $ yum install sqlite3

2. rabbitmq

   $ yum -y install rabbitmq-server
   $ chkconfig rabbitmq-server on
   $ service rabbitmq-server start

3. python pip + virtual env

   $ yum install python-pip python-virtualenv python-virtualenvwrapper.noarch

4. soda user

   $ adduser -c "SODA service user" -d /var/lib/soda -m --system -s /sbin/nologin soda
   $ visudo
   soda ALL= (root) NOPASSWD: /usr/sbin/rabbitmqctl
   $ su - soda -s /bin/bash

5. MARS client

must have a MARS client installed and be able to use list/retrieve
data from NSC:s MARS servers.

   $ yum install ksh libgfortran jasper openjpeg-libs
   $ rpm -ivh --nodeps grib_api*
   copy tags/smhi-grib_api-definition-$VERSION/* /opt/metapps/grib_api/share/samples/
   unpack ready made tarball of client into /var/lib/soda
   $ tar xf metapps.tar
   $ cd ~/bin; ln -s ../opt/metapps/mars/2.5/bin/mars
   verify correct MARSBASE and value of MARS_HOSTNAME in ../opt/metapps/mars/2.5/bin/mars
   verify correct paths in ../opt/metapps/mars/2.5/etc/mars{op,re}.cfg
   modify iptables to allow any incoming from marsop (vir) and marsre (lennier)
   make symlink from /var/lib/soda/opt/metapps/grib_api/share/definitions -> /opt/metapps/grib_api/share/

To test this works:

   $ echo 'list,class = op,stream = oper,expver = c11a,model = hirlam,type = fc,date = 20130601,time = 00,step =0,levtype = hl,levelist = 2,param = 11.1' | mars
   $ echo 'retrieve,class = op,stream = oper,expver = c11a,model = hirlam,type = fc,date = 20130601,time = 00,step =0,levtype = hl,levelist = 2,param = 11.1',target="removeme.grb" | mars

6. setup venv

   $ virtualenv venv
   $ . venv/bin/activate

7. install celery sqlalchemy flask + misc
   $ pip install sqlalchemy celery Flask Flask-SQLAlchemy simplejson pika

8. modify iptables to allow incoming to tcp 8000 from NSC network

9. initialize database schemas:

   $ python -c 'import soda; import models; models.db.create_all(app=soda.app)'


STARTING UP
===========

STAGEDIR=/tmp/soda python soda.py
celery --no-color -c1 -n scheduler --app=tasks.cel -Q scheduler worker --loglevel=warning --statedb=scheduler.state
celery --no-color -c1 -n worker1   --app=tasks.cel -Q default worker   --loglevel=warning --statedb=worker1.state

where the scheduler queue must have one and only one worker to avoid
errors from race conditions due to the db being modified concurrently
during scheduling operations. The default queue can have any number of
workers.

CLEANING STATE
==============

! Warning: these are dangerous operations and will nuke the whole
!          state of SODA. You have been warned.

1. First stop the flask web application and all the celery workers.

2. Delete celery queues:

   celery -f -A tasks.cel purge

3. Delete the web application database and all celery state files:

   rm /tmp/flask.db *.state celery_task.db

4. (Optional) Delete any activemq queues not explicitly declared by
   SODA (dangerous: assumes SODA is the _only_ user of this activemq
   instance)

   1. create a shell script or function amqp_list_q:

      function amqp_list_q() {
         sudo rabbitmqctl list_queues | awk '$1 !~ "^(celery|default|scheduler|Listing|\\.\\.\\.)" { print $1 }'
      }

   2. create a python program amqp_del_q:
      #!/usr/bin/env python
      import sys
      import pika

      connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

      if len(sys.argv) > 1:
          for q in sys.argv[1:]:
              connection.channel().queue_delete(q)
      else:
          for q in sys.stdin:
              connection.channel().queue_delete(q.strip())

   3. delete all queues not defined by SODA:

      amqp_list_q | amqp_del_q

5. Finally recreate the database schemas:

   python -c 'import soda; import models; models.db.create_all(app=soda.app)'


A complete (but dangerous) cleanup would be:

   celery -f -A tasks.cel purge
   amqp_list_q | amqp_del_q
   rm /tmp/flask.db *.state celery_task.db
   python -c 'import soda; import models; models.db.create_all(app=soda.app)'


A minimal cleanup is possible by only purging the entries in the SODA
SQLite db. This is quicker than the above and you don't have to
restart the web app and all the workers:

    sqlite3 /tmp/flask.db 'delete from download_request; delete from request_files; delete from stagable_file; delete from task;'
