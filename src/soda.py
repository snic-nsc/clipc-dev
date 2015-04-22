from celery import Celery
from celery.signals import after_task_publish
from datetime import datetime
from flask import abort, Flask, jsonify, request, redirect, url_for, send_from_directory
from flask.ext.sqlalchemy import SQLAlchemy
from signal import alarm, signal, SIGALRM
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func
from collections import OrderedDict
import simplejson as json
import hashlib
import logging
import kombu
import os
import os.path
import re
import select
import subprocess
import sys
import tempfile
import time
import uuid

# TODO: prioritise task execution so that whole requests finish first
# before taking on other, otherwise we may have deadlocks if we serve
# the downloadscript after all are ready

# TODO: implement space reservations

# TODO: /soda/v1/api/doc


# https://denibertovic.com/posts/celery-best-practices/
# https://news.ycombinator.com/item?id=7909201
# http://prschmid.blogspot.se/2013/04/using-sqlalchemy-with-celery-tasks.html
# TODO: allow XML and HTML repsonses besides JSON

logger = logging.getLogger('soda')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(name)s/%(levelname)s %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

app = Flask('soda')
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/flask.db'
db = SQLAlchemy(app)

CELERY_TASK_DB = './celery_task.db'

cel = Celery(app.name,
             backend='db+sqlite:///' + CELERY_TASK_DB,
             broker='amqp://guest@localhost//')
cel.conf.update(app.config)
cel.conf.update(
    # explicitly accept pickle to get rid of security warnings at startup:
    CELERY_ACCEPT_CONTENT = [ 'pickle', 'json', 'msgpack', 'yaml' ],

    # http://celery.readthedocs.org/en/latest/userguide/routing.html
    CELERY_QUEUES = (
        # rename default -> stager
        kombu.Queue('default',   kombu.Exchange('default'), routing_key='default'),   # >= 1 worker
        kombu.Queue('scheduler', kombu.Exchange('default'), routing_key='schedule'),  # == 1 worker
        ),
    CELERY_ROUTES = { 'soda.create_request' : { 'queue' : 'scheduler' } },
    CELERY_DEFAULT_QUEUE = 'default',
    CELERY_DEFAULT_EXCHANGE_TYPE = 'direct',
    CELERY_DEFAULT_ROUTING_KEY = 'default',
    )

# a many-to-many rel. see https://pythonhosted.org/Flask-SQLAlchemy/models.html
files = db.Table('request_files',
                 db.Column('download_request_uuid',
                           db.String(36),
                           db.ForeignKey('download_request.uuid')),
                 db.Column('pub_file_name',
                           db.String(255),
                           db.ForeignKey('pub_file.name')))


# http://stackoverflow.com/questions/9824172/find-out-whether-celery-task-exists
@after_task_publish.connect
def update_sent_state(sender=None, body=None, **kwargs):
    task = cel.tasks.get(sender)
    backend = task.backend if task else cel.backend
    backend.store_result(body['id'], None, "SUBMITTED")


# we possibly will need to include the query string also
class DownloadRequest(db.Model):
    uuid = db.Column(db.String(36), primary_key=True)
    openid = db.Column(db.Text, nullable=False)
    time_created = db.Column(db.DateTime, nullable=False)
    files = db.relationship('PubFile', secondary=files,
                            backref=db.backref('requests'))

    def __init__(self, openid):
        self.uuid = uuid.uuid4().get_hex()
        self.openid = openid
        self.time_created = datetime.utcnow()

    def __repr__(self):
        return '%s(uuid=%s, openid=%s, time_created=%s)' % \
            (type(self).__name__, self.uuid, self.openid, self.time_created)


TMPDIR = os.getenv('TMPDIR', '/tmp')

# FIXME: add constraint >= 0 for count_downloads ; http://stackoverflow.com/questions/14225998/flask-sqlalchemy-column-constraint-for-positive-integer
# http://docs.sqlalchemy.org/en/rel_0_9/orm/relationships.html
class PubFile(db.Model):
    name = db.Column(db.String(255), primary_key=True)
    size = db.Column(db.Integer)
    checksum_sha1 = db.Column(db.String(40))
    count_downloads = db.Column(db.Integer, nullable=False)
    path = db.Column(db.Text, nullable=False)
    time_accessed = db.Column(db.DateTime)
    task_uuid = db.Column(db.String(36), db.ForeignKey('task.uuid'))
    task = db.relationship('Task', uselist=False, backref='pub_file')

    def __init__(self, name, size=None, checksum_sha1=None, count_downloads=1,
                 path=TMPDIR):
        self.name = name
        self.size = size
        self.checksum_sha1 = checksum_sha1
        self.count_downloads = count_downloads
        self.path = path

    def __repr__(self):
        # size may be NULL
        return ( '%s(name=%s, size=%s, checksum_sha1=%s, count_downloads=%s, '
                 'time_accessed=%s)' % (type(self).__name__,
                                        self.name,
                                        self.size,
                                        self.checksum_sha1,
                                        self.count_downloads,
                                        self.time_accessed))

    def path_staged(self):
        return os.path.join(self.path, self.name)

    def is_staged(self):
        return os.path.exists(self.path_staged())

    def is_staging(self):
        return self.task and self.task.is_valid() and not self.task.is_done()



class Task(db.Model):
    uuid = db.Column(db.String(36), primary_key=True)
    path_stdout = db.Column(db.Text)
    path_stderr = db.Column(db.Text)

    def __init__(self, uuid, path_stdout, path_stderr):
        self.uuid = uuid
        self.path_stdout = path_stdout
        self.path_stderr = path_stderr

    def __repr__(self):
        return '%s(uuid=%s, path_stdout=%s, path_stderr=%s)' % \
            (type(self).__name__,
             self.uuid,
             self.path_stdout,
             self.path_stderr)

    def future(self):
        return cel.AsyncResult(self.uuid)

    # this works since we set status to submitted upon submission,
    # otherwise we would not be able to differentiate between unknown
    # tasks and tasks that have been submitted. Also, we depend on
    # late_acks for resubmission of any killed tasks
    def is_valid(self):
        return self.future().status != 'PENDING'

    def is_done(self):
        return self.future().ready()

    def is_failed(self):
        return self.future().ready() and self.future().status != 'SUCCESS'

    def get_result(self):
        return self.future().get()

    def stdout(self):
        with open(self.path_stdout) as f:
            return f.read()

    def stderr(self):
        with open(self.path_stderr) as f:
            return f.read()


def getEsgfQuery(request_args):
	where=os.path.dirname(__file__)
	facetfile=os.path.join(where,"esgf-mars-facet-mapping")
	mappingsfile=os.path.join(where,"esgf-mars-default-mapping")
	mappingsdict=OrderedDict()
	facetvals=dict()
	fp=open(facetfile,"r")
	lines=fp.readlines()
	fp.close()
	fp=open(mappingsfile,'r')
	mappingsdict=json.load(fp,object_pairs_hook=OrderedDict)
	fp.close()
	facetlist=list()
	for line in lines:
		facet=line.split(':')[0]
		facetlist.append(facet)
	try:
		for facet in facetlist:
			#print facet
			val=request_args.get(facet)
			if val!= None:
				#print "%s:%s"%(facet,val)
				facetvals[facet]=val
		mappingsdict['date']=mappingsdict['datestr']
		mappingsdict['date']+=str(mappingsdict['freq'])
		mappingsdict.pop('datestr')
		mappingsdict.pop('freq')
	except:
		raise
	return mappingsdict
		

@cel.task(acks_late=True)
def create_request(openid, files):
    """A Celery task which return a newly created DownloadRequest.
    The worker must be this must have one worker only to avoid concurrency issues 
    """

    # consider removing this complexity since we run this within a single worker
    while True:
        try:
            r = DownloadRequest(openid)
            logger.info('got new request %s' % r)
            for file_name in files:
                pf = PubFile.query.get(file_name)
                if pf:
                    # FIXME: verify this doesn't update twice in event of IntegrityError
                    pf.count_downloads = PubFile.count_downloads + 1
                    logger.debug('%s exists in db' % pf)
                else:
                    pf = PubFile(file_name)
                    logger.debug('%s does not exist in db' % pf)
                r.files.append(pf)
            db.session.add(r)
            db.session.commit()
            break
        # Make sure we handle the race condition of the file being
        # added concurrently:
        except IntegrityError, e:
            # TODO: find a more robust way of detecting whether the
            # column already exists:
            if re.match(r'.*column name is not unique', str(e)):
                logger.debug('got %s, rolling back and retrying...' % str(e))
                db.session.rollback()
            else:
                raise
    return r.uuid


class APIException(Exception):
    def __init__(self, rc, message, payload=None):
        Exception.__init__(self)
        self.rc = rc
        self.message = message
        self.payload = payload if payload is not None else {}

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return '%s(rc=%s, message=%s, payload=%s' % \
            ( type(self).__name__, self.rc, self.message, self.payload )


class HTTPBadRequest(APIException):
    def __init__(self, message, payload=None):
        APIException.__init__(self, 400, message, payload)

class HTTPUnauthorized(APIException):
    def __init__(self, message, payload=None):
        APIException.__init__(self, 401, message, payload)

class HTTPForbidden(APIException):
    def __init__(self, message, payload=None):
        APIException.__init__(self, 403, message, payload)

class HTTPNotfound(APIException):
    def __init__(self, message, payload=None):
        APIException.__init__(self, 404, message, payload)

class HTTPInternalError(APIException):
    def __init__(self, message, payload=None):
        APIException.__init__(self, 500, message, payload)

@app.errorhandler(APIException)
def page_api_exception(error):
    logger.debug(error)
    return ( jsonify(message=error.message, payload=error.payload),
             error.rc )

# Note: this won't be called if flask is running in debug mode
@app.errorhandler(500)
def page_api_internal_error(error):
    logger.error(str(error))
    return ( jsonify(message='Internal Error', payload=str(error)), 500 )



# FIXME: requires authorization (used by the esg node)
# TODO: maybe limit the amount of retries, timeout?
@app.route('/new_request', methods=['POST'])
def render_new_request():
    logger.debug(request)
    content_type = request.headers['Content-Type']
    if content_type != 'application/json':
        raise HTTPBadRequest('HTTP Content-Type must be application/json')
    if 'openid' not in request.json:
        raise HTTPBadRequest('expecting JSON attribute openid (string)')
    openid = request.json['openid']

    mars_request = request.json.get('mars_request', '')
    files = request.json.get('files', [ 'mars_' + hashlib.sha1(mars_request).hexdigest() + '.grb' ])

    if not files and not request:
        raise HTTPBadRequest('expecting JSON attribute files (list of strings) or mars_request', payload=str(request)) # FIXME: include a pretty version of request

    r_uuid = create_request.delay(openid, files).get()
    r = DownloadRequest.query.get(r_uuid)

    # We can either create a db task before executing the task or the
    # other way around. This is important when considering concurrent
    # downloads. Since it's hard to make sure we have only one task in
    # the db and also to guarantee that it is actually running, we
    # choose risking more than one concurrent task in parallel, since
    # it's simpler and pretty harmless. Note also that there is a
    # one-to-one rel. between a file and a task.

    staged_files = [ pf for pf in r.files if pf.is_staged() ]
    staging_files = [ pf for pf in r.files if pf.is_staging() ]
    unstaged_files = [ pf for pf in r.files if (pf.task and pf.task.is_failed()) or (not pf.is_staged() and not pf.is_staging()) ]

    logger.debug('staged files: %s' % staged_files)
    logger.debug('staging files: %s' % [ (f, f.task, f.task.future().status) for f in staging_files ])
    logger.debug('unstaged files: %s' % unstaged_files)

    # FIXME: handle ioexception or write into user writable dir only,
    # celery_uuid.{out,err}
    for pf in unstaged_files:
        _, path_stdout = tempfile.mkstemp()
        _, path_stderr = tempfile.mkstemp()
        # race condition: there's a risk of more than one concurrent identical request here
        assert mars_request, 'TODO: create proper request from the file'

        async_result = stage_file.delay(mars_request, pf.path_staged(), path_stdout,
                                        path_stderr)
        logger.debug('executed task %s' % async_result)
        task = Task(async_result.id, path_stdout, path_stderr)
        pf.task = task
        logger.debug('created task %s in db' % task)

        #TODO: test with time.sleep(10) and concurrent request for same files when having a concurrent web server

    db.session.commit() # or internal error

#    orphans = Task.query.filter(Task.pub_file is None)
#    for t in orphans:
#        # there's a risk this misses and kills the wrong task if a new one gets to start
#        logger.debug('killing orphan task %s' % t)
#        t.future().revoke(Terminate=True)

    # FIXME: celery throw exception
    # FIXME: celery stream stdout
    # https://github.com/celery/celery/issues/1462
    # FIXME: celery notify by mail

    return jsonify(), 201, { 'location': '/request/%s' % r.uuid }


@app.route('/new_request_demo', methods=['POST'])
def render_new_request_demo():
    logger.debug(request)
    logger.debug(request.headers)
    content_type = request.headers['Content-Type']
    if content_type != 'application/x-www-form-urlencoded':
        raise HTTPBadRequest('HTTP Content-Type must be application/x-www-form-urlencoded but is %s' % content_type)

    logger.debug('request.args: ' + str(request.args))
    q = getEsgfQuery(request.args)
    params = ','.join('%s=%s' % (k,v) for k,v in q.iteritems())
    file_name = 'mars_%s.grb' % hashlib.sha1(params).hexdigest()
    mars_request = "retrieve,%s,target='%s/%s.tmp'" % (params, TMPDIR, file_name)  # FIXME: don't hardcode TMPDIR here
    logger.debug(mars_request)

    files = [ file_name ]
    openid = 'https://esg-dn1.nsc.liu.se/esgf-idp/openid/perl'

    r_uuid = create_request.delay(openid, files).get()
    r = DownloadRequest.query.get(r_uuid)

    # We can either create a db task before executing the task or the
    # other way around. This is important when considering concurrent
    # downloads. Since it's hard to make sure we have only one task in
    # the db and also to guarantee that it is actually running, we
    # choose risking more than one concurrent task in parallel, since
    # it's simpler and pretty harmless. Note also that there is a
    # one-to-one rel. between a file and a task.

    staged_files = [ pf for pf in r.files if pf.is_staged() ]
    staging_files = [ pf for pf in r.files if pf.is_staging() ]
    unstaged_files = [ pf for pf in r.files if (pf.task and pf.task.is_failed()) or (not pf.is_staged() and not pf.is_staging()) ]

    logger.debug('staged files: %s' % staged_files)
    logger.debug('staging files: %s' % [ (f, f.task, f.task.future().status) for f in staging_files ])
    logger.debug('unstaged files: %s' % unstaged_files)

    # FIXME: handle ioexception or write into user writable dir only,
    # celery_uuid.{out,err}
    for pf in unstaged_files:
        _, path_stdout = tempfile.mkstemp()
        _, path_stderr = tempfile.mkstemp()
        # race condition: there's a risk of more than one concurrent identical request here
        async_result = stage_file.delay(mars_request, pf.path_staged(), path_stdout,
                                        path_stderr)
        logger.debug('executed task %s' % async_result)
        task = Task(async_result.id, path_stdout, path_stderr)
        pf.task = task
        logger.debug('created task %s in db' % task)

        #TODO: test with time.sleep(10) and concurrent request for same files when having a concurrent web server

    db.session.commit() # or internal error

#    orphans = Task.query.filter(Task.pub_file is None)
#    for t in orphans:
#        # there's a risk this misses and kills the wrong task if a new one gets to start
#        logger.debug('killing orphan task %s' % t)
#        t.future().revoke(Terminate=True)

    # FIXME: celery throw exception
    # FIXME: celery stream stdout
    # https://github.com/celery/celery/issues/1462
    # FIXME: celery notify by mail

    return jsonify(), 201, { 'location': '/request/%s' % r.uuid }



# FIXME: only to be called by matching openid
@app.route('/request/<uuid>', methods=['GET'])
def render_get_request(uuid):
    #assert request.headers['Content-Type'] == 'application/json', request
    #assert 'openid' in request.json, request.json
    r = DownloadRequest.query.get_or_404(uuid)
    all_files = set(r.files)
    staged = set(f for f in all_files if f.is_staged())
    offline = all_files - staged
    progress = float(len(staged)) / len(r.files)
    is_done = len(offline) == 0
    urls_offline_files = [ url_for('render_get_offline_file_status', uuid=r.uuid, file_name=x.name, _external=True) for x in offline ]
    urls_staged_files = [ url_for('render_get_staged_file', uuid=r.uuid, file_name=x.name, _external=True) for x in staged ]
    return jsonify(is_done=is_done,
                   progress=progress,
                   staged_files=urls_staged_files,
                   offline_files=urls_offline_files), 200, { 'location' : '/request/%s' % r.uuid }

@app.route('/request/<uuid>/offline/<file_name>', methods=['GET'])
def render_get_offline_file_status(uuid, file_name):
    r = DownloadRequest.query.get_or_404(uuid)
    # FIXME: query r for files in r directly
    f = PubFile.query.get_or_404(file_name)
    assert r in f.requests, (r, f)
    # TODO: if is done do redirect to staged? or provide link?
    return jsonify(stdout=f.task.stdout(), stderr=f.task.stderr()), 200



#return jsonify(stdout=f.task.stdout() if f.task else ""), 200

# set time of cache expiry?
# use some reference counting scheme? but we cannot reliably detect
# successful downloads? we'd also avoid derefence twice for the same
# file and request

# possibly use http content disposition?
@app.route('/request/<uuid>/staged/<file_name>', methods=['GET'])
def render_get_staged_file(uuid, file_name):
    # FIXME: find out what happens if PubFile.count_downloads is
    # concurrently being modified:
    f = PubFile.query.get_or_404(file_name)
    f.time_accessed = func.now()
    db.session.commit()
    return redirect(url_for('render_static', uuid=uuid, file_name=file_name))

# FIXME: REMOVEME: should be served from apache or whatever will be
# running in production
# SHOULD contain authorization, you're not allowed to download any
# files just like that even if they happen to be available.
# X-Sendfile
# http://www.yiiframework.com/wiki/129/x-sendfile-serve-large-static-files-efficiently-from-web-applications/
# http://pythonhosted.org/xsendfile/
# FIXME: lock while serving so it's not expired by the cleaner?
@app.route('/static/<uuid>/staged/<file_name>', methods=['GET'])
def render_static(uuid, file_name):
    r = DownloadRequest.query.get_or_404(uuid)
    f = PubFile.query.get_or_404(file_name)
    assert r in f.requests, (r, f)
    return send_from_directory(f.path, f.name)

def fd_read_outerr(fd_out, fd_err):
    rlist = [fd_out, fd_err]
    while rlist:
        rready, _, _ = select.select(rlist, [], [])
        for fd in rready:
            data = os.read(fd.fileno(), select.PIPE_BUF) # read at most PIPE_BUF bytes
            if data:
                yield fd, data
            else:
                rlist.remove(fd)

class TimeoutException(Exception): pass

def raise_timeout_exception(signum, frame):
    raise TimeoutException

def timed_wait(p, timeout):
    signal(SIGALRM, raise_timeout_exception)
    alarm(timeout)
    rc = p.wait()
    alarm(0)
    return rc

def exec_proc(cmd, path_out, path_err, stdin=None, term_timeout=5):

    assert type(cmd) is list, 'cmd %s' % cmd
    assert all(type(x) is str for x in cmd), 'cmd %s' % cmd
    assert type(path_out) is str, 'path_out %s' % path_out
    assert type(path_err) is str, 'path_err %s' % path_err
    assert type(stdin) is None or type(stdin) is str or type(stdin) is unicode, 'stdin %s' % type(stdin)
    assert term_timeout >= 0, 'term_timeout %s' % term_timeout

    logger.debug('executing %s (stdout=%s, stderr=%s)' % (cmd, path_out, path_err))

    LINE_BUFFERED = 1
    p = subprocess.Popen(cmd,
                         shell=False,
                         bufsize=LINE_BUFFERED,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

    # FIXME: make sure killing the MARS process also kills the tunnel
    logger.debug('pid is %d' % p.pid)

    try:
        with open(path_out, 'w') as f_out:
            with open(path_err, 'w') as f_err:
                # FIXME: split into digestable chunks, otherwise we may deadlock
                if stdin is not None:
                    p.stdin.write(stdin)
                    p.stdin.flush()
                    p.stdin.close()
                for fd, data in fd_read_outerr(p.stdout, p.stderr):
                    f = f_out if fd == p.stdout else f_err
                    f.write(data)
                    # FIXME: only flush every certain interval (make configurable)
                    f.flush()
                    if f == f_out:
                        logger.debug('%d out: %s' % (p.pid, data.strip()))
                    else:
                        logger.debug('%d err: %s' % (p.pid, data.strip()))
                f_err.flush()
                os.fdatasync(f_err.fileno())
            f_out.flush()
            os.fdatasync(f_out.fileno())
        rc = p.wait()
    except Exception, e:
        logger.error(e)
        logger.debug('terminating process %s' % p)
        p.terminate()
        try:
            rc = timed_wait(p, term_timeout)
        except TimeoutException:
            logger.warn('timed out (after %d s.) waiting for process %s to '
                        'terminate - sending SIGKILL' % ( term_timeout, p ))
            p.kill()
            rc = p.wait()

    return rc

@cel.task(acks_late=True)
def stage_file(request, result_file, path_out, path_err):

    # task_id = stage_file.request.id
    if os.path.exists(result_file):
        return 0

    # FIXME: if the requeust is faulty in some ways we need a way to
    # detect this and timeout otherwise it will keep on going forever

    rc = exec_proc([ 'mars' ], path_out, path_err, stdin=request)
    # FIXME: handle errors
    logger.debug('mars returned %d' % rc)

    if rc == 0:
        # FIXME: handle errors and don't hardcode tempname like this
        os.rename(result_file + '.tmp', result_file)



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
