requirements
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

   $ python -c 'import soda; soda.db.create_all()'


STARTING UP
===========

STAGEDIR=/tmp/soda python soda.py
celery --no-color -c1 -n registrar --app=soda.cel -Q registrar worker --loglevel=warning --statedb=registrar.state -- celeryd.prefetch_multiplier=1
celery --no-color -c1 -n scheduler --app=soda.cel -Q scheduler worker --loglevel=warning --statedb=scheduler.state -- celeryd.prefetch_multiplier=1
celery --no-color -c1 -n worker1   --app=soda.cel -Q default worker   --loglevel=warning --statedb=worker1.state   -- celeryd.prefetch_multiplier=1

where the queues registrar and scheduler must have one and only one
worker, but the default can have one or more workers assigned to it.

CLEANING STATE
==============
rm *.state /tmp/flask.db; python -c 'import soda; soda.db.create_all()'
