from __future__ import absolute_import
from collections import OrderedDict
from celery.result import allow_join_result
from celery.utils.log import get_task_logger
from datetime import datetime
from flask import Flask, jsonify, redirect, request, send_from_directory, \
    url_for
from flask.ext.sqlalchemy import SQLAlchemy
from signal import alarm, SIGALRM, signal
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import func
import celery
import errno
import hashlib
import logging
import kombu
import os
import os.path
import re
import select
import simplejson as json
import subprocess
import tempfile
import uuid

# https://github.com/geopython/PyWPS/pull/26/files
# http://prschmid.blogspot.se/2013/04/using-sqlalchemy-with-celery-tasks.html
# http://stackoverflow.com/questions/16792032/sqlalchemy-session-issues-with-celery

# disable db repeatable read to improve performance
# FIXME: update _repr_ for all db model
# FIXME: make sure logging from tasks prefixes with task id
# TODO: provide user request cancellation (by providing a html link for that in the request page)
# TODO: see if we can implement task cancellation with amqp
# TODO: Make sure workers run as a separate user with restricted
#       rights to reduce the amount of damage unknown input can cause
# TODO: consistently handle unicode input

# TODO: make acks_late=True default for all tasks with a single config
#       rather than decorating each method
# TODO: > 1 filesystems as cache space and possibly allow distributed
#       staging workers (which would prevent using single system
#       mutexes like semaphore or flock)
# TODO: /soda/v1/api/doc
# https://denibertovic.com/posts/celery-best-practices/
# https://news.ycombinator.com/item?id=7909201
# TODO: allow XML and HTML repsonses besides JSON



# log to syslog: http://www.toforge.com/2011/06/celery-centralized-logging/
LOGFORMAT='%(asctime)s %(levelname)-7s %(message)s'

if __name__ == '__main__':
    logger = logging.getLogger('soda')
    ch = logging.StreamHandler()
    #ch.setFormatter(logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s', datefmt='%Y-%m-%dT%H:%M:%S'))
    ch.setFormatter(logging.Formatter(fmt=LOGFORMAT))
    logger.addHandler(ch)
else:
    logger = get_task_logger(__name__)

#logger.setLevel(logging.DEBUG)
app = Flask('soda')
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/flask.db'
db = SQLAlchemy(app)


CELERY_TASK_DB = './celery_task.db'

cel = celery.Celery(app.name,
#                    backend='db+sqlite:///' + CELERY_TASK_DB, # disabled for now since we spurious db integrityerrors
                    backend='amqp://guest@localhost//',
                    broker='amqp://guest@localhost//')
cel.conf.update(app.config)
cel.conf.update(
#    CELERY_RESULT_ENGINE_OPTIONS = { 'echo': True },
    # explicitly accept pickle (which we need to be able to serialize
    # python exceptions etc.) to get rid of security warnings at
    # startup):
    CELERYD_TASK_LOG_FORMAT = LOGFORMAT,
    CELERYD_LOG_FORMAT = '%(asctime)s %(levelname)-7s <celery> %(message)s',
    CELERY_ACCEPT_CONTENT = [ 'pickle', 'json', 'msgpack', 'yaml' ],
    CELERY_QUEUES = (
        kombu.Queue('default',
                    kombu.Exchange('default'),
                    routing_key='default'),    # >= 1 worker
        # we have separate workers for register_request and other
        # schedule tasks to be able to quickly respond to creation of
        # new requests (by not blocking scheduler to block
        # schedule_tasks and vice versa)
        kombu.Queue('scheduler',
                    kombu.Exchange('default'),
                    routing_key='schedule'),   # == 1 worker
        kombu.Queue('registrar',
                    kombu.Exchange('default'),
                    routing_key='register')    # == 1 worker
    ),
    CELERY_ROUTES = { 'soda.register_request_demo' : { 'queue' : 'registrar' },
                      'soda.register_request' : { 'queue' : 'registrar' },
                      'soda.schedule_tasks' : { 'queue' : 'scheduler' },
                      'soda.schedule_join_staging_task' : { 'queue' : 'scheduler' },
                      'soda.schedule_mark_request_deletable' : { 'queue' : 'scheduler' },
                      'soda.schedule_submit_sizing_tasks' : { 'queue' : 'scheduler' } },
    CELERY_DEFAULT_QUEUE = 'default',
    CELERY_DEFAULT_EXCHANGE_TYPE = 'direct',
    CELERY_DEFAULT_ROUTING_KEY = 'default'
)


class CommandFailed(Exception): pass
class TaskFailure(Exception): pass
class UnknownTask(Exception): pass

def df(mp):
    # $ df -P --block-size=1 .
    # Filesystem                     1-blocks       Used   Available Capacity Mounted on
    # /dev/mapper/vg_vbox-lv_root 13613391872 2734968832 10180071424      22% /
    #
    # -> { 'Available': '22181310464', 'Used': '89328218112', 'Capacity': '81%',
    #      'Filesystem': '/dev/mapper/kubuntu--vg-root',
    #      '1-blocks': '117501927424', 'Mounted on': '/' }
    cmd = [ 'df', '-P', '--block-size=1', mp ]
    p = subprocess.Popen(cmd,
                         shell=False,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    out, err = p.communicate()
    rc = p.wait()
    if rc != 0:
        raise CommandFailed('%s failed: %s' % (' '.join(cmd), err))
    header, row = out.splitlines()
    cols = row.split()
    split_count = len(cols) - 1
    split_white_remove_empty = None
    keys = header.split(split_white_remove_empty, split_count)
    return dict(zip(keys, cols))


STAGEDIR = os.path.realpath(os.getenv('STAGEDIR', os.getenv('TMPDIR', '/tmp')))
# Assumes we own the whole filesystem. Possibly we should add a way to
# reserve a percentage or static amount of STAGE_SPACE rather than use
# all of it:
STAGE_SPACE = 459427 * 4 + 17 #int(df(STAGEDIR)['1-blocks'])

def register_existing_files(names):
    pass

def create_tempfile():
    fd, file_name = tempfile.mkstemp()
    os.close(fd)
    return file_name


@celery.signals.worker_init.connect
def establish_db_session(signal, sender):
    global cel_db_session
    engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'], echo=False)
    cel_db_session = scoped_session(sessionmaker(autocommit=False,
                                                 autoflush=True,
                                                 bind=engine))

@celery.signals.task_postrun.connect
def remove_db_session(**kwargs):
    cel_db_session.remove()

# http://stackoverflow.com/questions/9824172/find-out-whether-celery-task-exists
@celery.signals.after_task_publish.connect
def update_sent_state(sender=None, body=None, **kwargs):
    task = cel.tasks.get(sender)
    backend = task.backend if task else cel.backend
    backend.store_result(body['id'], None, "REGISTERED")


# a many-to-many rel. see https://pythonhosted.org/Flask-SQLAlchemy/models.html
request_files = db.Table('request_files',
                         db.Column('download_request_uuid',
                                   db.String(36),
                                   db.ForeignKey('download_request.uuid')),
                         db.Column('stagable_file_name',
                                   db.String(255),
                                   db.ForeignKey('stagable_file.name')))


class DownloadRequest(db.Model):
    uuid = db.Column(db.String(36), primary_key=True)
    openid = db.Column(db.Text, nullable=False)
    time_created = db.Column(db.DateTime, nullable=False)
    files = db.relationship('StagableFile', secondary=request_files,
                            backref=db.backref('requests'))
    state = db.Column(db.Enum('created', 'dispatched', 'finished', 'failed'),
                      nullable=False, default='created')
    is_deletable = db.Column(db.Boolean, default=False, nullable=False)

    def __init__(self, openid):
        self.uuid = uuid.uuid4().get_hex()
        self.openid = openid
        self.time_created = datetime.utcnow()

    def __repr__(self):
        return ( '%s(uuid=%s, openid=%s, time_created=%s, state=%s, '
                 'is_deletable=%s)' % \
                 ( type(self).__name__, self.uuid, self.openid,
                   self.time_created, self.state, self.is_deletable ))


# FIXME: add constraint >= 0 for request_count ;
# http://stackoverflow.com/questions/14225998/flask-sqlalchemy-column-constraint-for-positive-integer
#
# doc: transparently inserted upon first creation and persistent even
# if not referenced in a downloadrequest
class StagableFile(db.Model):
    name = db.Column(db.String(255), primary_key=True)
    size = db.Column(db.Integer)
    params = db.Column(db.Text)
    checksum_sha1 = db.Column(db.String(40))
    request_count = db.Column(db.Integer, nullable=False)
    time_accessed = db.Column(db.DateTime)
    path = db.Column(db.Text, nullable=False) # rename to dir/directory?
    staging_task_uuid = db.Column(db.String(36), db.ForeignKey('task.uuid'))
    sizing_task_uuid = db.Column(db.String(36), db.ForeignKey('task.uuid'))
    staging_task = db.relationship('Task', foreign_keys=[staging_task_uuid],
                                   uselist=False, backref='stagable_file')
    sizing_task = db.relationship('Task', foreign_keys=[sizing_task_uuid],
                                  uselist=False)
    state = db.Column(db.Enum('online', 'offline'),
                      nullable=False, default='offline')

    def __init__(self, name, params=None, size=None, checksum_sha1=None, request_count=1,
                 path=STAGEDIR):
        self.name = name
        self.params = params
        self.size = size
        self.checksum_sha1 = checksum_sha1
        self.request_count = request_count
        self.path = path

    def __repr__(self):
        return ( '%s(name=%s, state=%s, size=%s, checksum_sha1=%s, request_count=%s, '
                 'time_accessed=%s, path=%s)' % \
                 ( type(self).__name__, self.name, self.state, self.size,
                   self.checksum_sha1, self.request_count, self.time_accessed,
                   self.path ))

    def path_staged(self):
        return os.path.join(self.path, self.name)


# possibly use delete delete-orphan cascade?
class Task(db.Model):
    uuid = db.Column(db.String(36), primary_key=True)
    path_stdout = db.Column(db.Text)
    path_stderr = db.Column(db.Text)

    def __init__(self, uuid, path_stdout=None, path_stderr=None):
        self.uuid = uuid
        self.path_stdout = path_stdout
        self.path_stderr = path_stderr

    def __repr__(self):
        return '%s(uuid=%s, path_stdout=%s, path_stderr=%s)' % \
            ( type(self).__name__,
              self.uuid,
              self.path_stdout,
              self.path_stderr )

    def get(self):
        if self.is_registered():
            return self.future().get()
        else:
            raise UnknownTask('Unknown Celery task id %s' % self.uuid)

    def future(self):
        return cel.AsyncResult(self.uuid)

    # we can't use terminate=True to kill the task since this may kill
    # the wrong task if the intended receipient finishes before being
    # terminated, so this will just revoke not already started tasks
    #
    # TODO: find out whether we can terminate by using amqp
    # or we register the pid of the mars process and kill that (assuming we can deliver this to the same worker
    def cancel(self):
        self.future().revoke()

    # in update_sent_state we set status to REGISTERED upon submission
    # of a new task which makes it possible to differentiate between
    # unknown tasks (PENDING) and tasks that have actually been
    # submitted.
    def is_registered(self):
        return self.future().status != 'PENDING'

    def is_done(self):
        return self.future().ready()

    def is_failed(self):
        return self.future().ready() and self.future().status != 'SUCCESS'

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


@celery.task(acks_late=True)
def register_request_demo(openid, file_to_query):
    r = DownloadRequest(openid)
    logger.debug('created new request %s' % r)
    file_names = file_to_query.keys()
    registered_files = cel_db_session.query(StagableFile).\
        filter(StagableFile.name.in_(file_names))
    logger.debug('registered files: %s' % registered_files.all())
    new_file_names = set(file_names) - set(x.name for x in registered_files)
    logger.debug('new file names: %s' % new_file_names)
    new_files = [ StagableFile(file_name, file_to_query[file_name]) for file_name in new_file_names ]
    logger.debug('new files: %s' % new_files)
    registered_files.update(
        { StagableFile.request_count : StagableFile.request_count  + 1 },
        synchronize_session=False)
    r.files.extend(registered_files)
    r.files.extend(new_files)
    # This won't throw an integrityerror as long as this task is
    # single threaded. If not, there's always a risk that one or more
    # identical StagableFile may be inserted concurrently.
    cel_db_session.add(r)
    cel_db_session.commit()
    logger.info('registered request %s for openid=%s, file_names=%s '
                '(unregistered=%s)' % \
                (r.uuid, openid, file_names, new_file_names))
    return r.uuid

@celery.task(acks_late=True)
def register_request(openid, file_names):
    r = DownloadRequest(openid)
    logger.debug('created new request %s' % r)
    registered_files = cel_db_session.query(StagableFile).\
        filter(StagableFile.name.in_(file_names))
    logger.debug('registered files: %s' % registered_files.all())
    new_file_names = set(file_names) - set(x.name for x in registered_files)
    logger.debug('new file names: %s' % new_file_names)
    new_files = [ StagableFile(file_name) for file_name in new_file_names ]
    logger.debug('new files: %s' % new_files)
    registered_files.update(
        { StagableFile.request_count : StagableFile.request_count  + 1 },
        synchronize_session=False)
    r.files.extend(registered_files)
    r.files.extend(new_files)
    # This won't throw an integrityerror as long as this task is
    # single threaded. If not, there's always a risk that one or more
    # identical StagableFile may be inserted concurrently.
    cel_db_session.add(r)
    cel_db_session.commit()
    logger.info('registered request %s for openid=%s, file_names=%s '
                '(unregistered=%s)' % \
                (r.uuid, openid, file_names, new_file_names))
    return r.uuid


# How long a request should exist in the db after it is finished or
# failed (in seconds). Only finished requests will keep reserving file
# space until deleted. Any files solely belonging to failed or
# deletable requests will be eligible for purging.
#REQUEST_PINNING_TIME = 24 * 3600
REQUEST_PINNING_TIME = 60
FILE_SIZE_EXTRA = 256
FILE_SIZE_WEIGHT = 1


# raises OSError if failing to delete a file except when it does not exist
# updates db
def purge_files(min_size, purgable_files):

    # we try to keep the cache as full as possible and defer deleting
    # any files until we can fullfil an allocation request completely
    # - since no request will start opportunisticly anyway. Also only
    # delete enough to ensure min_size bytes.
    bytes_freed = 0
    bytes_freeable = sum(sf.size for sf in purgable_files)
    requests_to_delete = set()

    if min_size <= bytes_freeable:
        for sf in purgable_files:
            if bytes_freed >= min_size:
                break
            logger.debug('deleting %s' % sf)
            # raises OSError on any error except if not existing:
            if sf.staging_task:
                unlink(sf.staging_task.path_stdout)
                unlink(sf.staging_task.path_stderr)
            is_deleted = unlink(sf.path_staged())
            if not is_deleted:
                logger.warn('tried purging staged file %s - but it is already '
                            'gone' % sf)
            sf.state = 'offline'
            bytes_freed += sf.size
            requests_to_delete.update(set(sf.requests))

    return bytes_freed, requests_to_delete

# on demand estimation of file size. Will only be done once for every
# file and will be updated with the actual size once the file has
# actually been staged (every time it differs from the registered
# value):
@celery.task(acks_late=True, ignore_results=True)
def schedule_submit_sizing_tasks(r_uuid):
    files_unknown_size = cel_db_session.query(StagableFile).\
        join(StagableFile.requests).\
        filter(DownloadRequest.uuid == r_uuid).\
        filter(StagableFile.size == None).\
        filter(StagableFile.sizing_task == None)
    logger.info('=> submitting sizing tasks for request %s: %s' % \
                (r_uuid, ', '.join(sf.name for sf in files_unknown_size)))
    for sf in files_unknown_size:
        async_result = estimate_size.delay(sf.name)
        sf.sizing_task = Task(async_result.id)
        logger.debug('submitted: %s' % sf.sizing_task)
    cel_db_session.commit()

# wait for the result of all sizing tasks, update the db with the
# result and delete the sizing task entry from the db. Requires a
# commit when done. Rraises any exception that occurred in the task
def join_sizing_tasks(files):
    with allow_join_result():
        for sf in files:
            try:
                assert sf.size is None, sf
                logger.debug('<= awaiting size estimation result for %s' % sf)
                if sf.sizing_task is None:
                    # this means it failed before and the task got
                    # unregistered from sf
                    raise TaskFailure('size estimation has failed - a new task '
                                      'will be created on the next request '
                                      'containing this file')
                size = sf.sizing_task.get()
                logger.debug('size estimated to %d bytes' % size)
                assert size >= 0, size
                sf.size = size
            except Exception, e:
                logger.warning('size estimation task %s failed: %s' % \
                               (sf.sizing_task, e))
                raise e
            finally:
                logger.debug('unregistering size estimation task %s' % \
                             (sf.sizing_task))
                cel_db_session.delete(sf.sizing_task)
                sf.sizing_task = None


@celery.task(acks_late=True, ignore_results=True)
def schedule_mark_request_deletable(r_uuid):
    try:
        r = cel_db_session.query(DownloadRequest).get(r_uuid)
        logger.info('marking %s request %s as deletable' % (r.state, r_uuid))
        r.is_deletable = True
        cel_db_session.commit()
    finally:
        # since there might be pending requests that wait for space to
        # be freed up:
        logger.debug('=> invoking scheduler')
        schedule_tasks.delay()

def notify_user(r, msg=None):
    pass
    #logger.error('TODO: NOT IMPLEMENTED: notify_user')

def finish_request(r):
    r.state = 'finished'
    notify_user(r, 'Request %s finished OK' % r)
    schedule_mark_request_deletable.apply_async(args=[r.uuid],
                                                countdown=REQUEST_PINNING_TIME)

def staging_tasks_owned_by(r):
    tasks = cel_db_session.query(Task).\
        join(StagableFile.staging_task).\
        join(StagableFile.requests).\
        filter(~StagableFile.requests.any(DownloadRequest.uuid != r.uuid))
    assert all(x.uuid == r.uuid for t in tasks for x in t.requests), (r, res)
    return tasks


def fail_request(r, msg):
    r.state = 'failed'
    for t in staging_tasks_owned_by(r):
        logger.debug('revoking task %s belonging only to failed request %s' % \
                     (t, r))
        t.cancel()
    notify_user(r, msg)
    schedule_mark_request_deletable.apply_async(args=[r.uuid],
                                                countdown=REQUEST_PINNING_TIME)



# we don't have to consider state 'created' even if a file belonging
# to a request may be staged from another request while the request
# still is in state 'created' (since we have separate workers for
# schedule_tasks and creation of new requests). The schedule_tasks
# will trigger the finishing of requests in state 'created' if all of
# the files have already been staged.
def finishable_requests(sf):
    res = dispatched_requests(sf).\
        filter(~DownloadRequest.files.any(StagableFile.state == 'offline'))

    assert set(res) == set(r for r in sf.requests \
                               if r.state == 'dispatched' and \
                               all(x.state == 'online' for x in r.files)),\
                               (requests, sf.requests)
    return res


def dispatched_requests(sf):
    res = cel_db_session.query(DownloadRequest).\
        join(DownloadRequest.files).\
        filter(StagableFile.name == sf.name).\
        filter(DownloadRequest.state == 'dispatched')

    assert set(res) == set(r for r in sf.requests \
                               if r.state == 'dispatched'),\
                               (requests, sf.requests)
    return res


# compute real file size and possibly update this and log if it
# differs from the estimated file size:
#
# raises OSError if not existing for some reason:
def update_size_if_different(sf):
    real_size = os.stat(sf.path_staged()).st_size
    size_diff_ratio_allowed = 1
    size_diff_ratio = abs(real_size - sf.size) / float(real_size)
    if size_diff_ratio > size_diff_ratio_allowed:
        logger.warning('reported file size %d and actual file size %d '
                       'differs with more than %d%%' % \
                       (sf.size, real_size, int(100 * size_diff_ratio)))
    if sf.size != real_size:
        logger.debug('updating db file size %d -> %d' % (sf.size, real_size))
        sf.size = real_size


@celery.task(acks_late=True, ignore_results=True)
def schedule_join_staging_task(task_id):
    # the task entry might not have been commited into the db yet
    while True:
        staging_task = cel_db_session.query(Task).get(task_id)
        if staging_task:
            break
        time.sleep(0.1)
    logger.debug('staging task for %s is: %s' % (task_id, staging_task))
    # BUG: FIXME: why is this a list and not a file, we do use uselist=False
    sf = staging_task.stagable_file[0]
    logger.info('staging task %s completed, file is %s' % (task_id, sf.name))

    try:
        with allow_join_result():
            logger.debug('getting staging result')
            staging_task.get() # returns None, but more importantly propagates any exception in the task
            logger.debug('updating file size if necessary')
            update_size_if_different(sf)
            logger.info('%s is online' % sf.name)
            sf.state = 'online'
            logger.debug('deregistering %s from %s' % (sf.staging_task, sf))
            cel_db_session.delete(sf.staging_task)
            unlink(sf.staging_task.path_stdout)
            unlink(sf.staging_task.path_stderr)
            sf.staging_task = None
            assert sf.staging_task is None, sf
            logger.debug('db commit')
            cel_db_session.commit() # verify that we really need this - but it's important that the query below includes r
            for r in finishable_requests(sf):
                logger.info('request %s finished' % r.uuid)
                finish_request(r)
    except Exception, e: # fixme: catch some, reraise others
        logger.warning('%s failed: %s' % (staging_task, e))
        # consider resubmitting the task a limited amount since it
        # should only fail in rare cases
        logger.debug('deregistering %s from %s' % (sf.staging_task, sf))
        cel_db_session.delete(staging_task)
        sf.staging_task = None
        assert sf.staging_task is None, sf
        logger.debug('db commit')
        cel_db_session.commit()
        for r in dispatched_requests(sf):
            logger.info('request %s failed' % r.uuid)
            fail_request(r, 'Staging of %s failed: %s' % (sf, str(e)))
    finally:
        cel_db_session.commit()


def get_online_files():
    return cel_db_session.query(StagableFile).\
        filter(StagableFile.state == 'online')


def get_reserved_files():
    return cel_db_session.query(StagableFile).\
        join(StagableFile.requests).\
        filter(~DownloadRequest.is_deletable).\
        filter(DownloadRequest.state.in_([ 'dispatched', 'finished' ]))

# Note: an active (dispatched/finished) request will keep reserving
# file space until explicitly being removed or marked as deletable in
# the db. A failed request will be present in the db until deleted,
# but its files won't be reserved.
def get_reserved_space():
    return sum(sf.size for sf in get_reserved_files())
#    reserved_space = cel_db_session.query(func.sum(StagableFile.size)).\
#        join(StagableFile.requests).\
#        filter(~DownloadRequest.is_deletable).\
#        filter(DownloadRequest.state.in_([ 'dispatched', 'finished' ])).\
#        scalar()
#    if reserved_space is not None:
#        return reserved_space
#    return 0


# return all StagableFile where either it is an orphan belonging to no
# request, or all its requests are either deletable or failed
def get_purgable_files():
    q_files_all_requests_failed_or_deletable = cel_db_session.query(StagableFile).\
        join(StagableFile.requests).\
        filter(StagableFile.state == 'online').\
        filter(~StagableFile.requests.any(or_(~DownloadRequest.state == 'failed',
                                               ~DownloadRequest.is_deletable)))
    q_files_no_requests = cel_db_session.query(StagableFile).\
        filter(StagableFile.state == 'online').\
        filter(~StagableFile.requests.any())
    purgable_files = q_files_all_requests_failed_or_deletable.\
        union(q_files_no_requests)
    assert all(r.is_deletable \
                   for sf in q_files_all_requests_failed_or_deletable \
                   for r in sf.requests), \
        '\n'.join('%s: %s' % (sf,sf.requests) \
                      for sf in q_files_all_requests_failed_or_deletable)
    assert all(len(sf.requests) == 0 for sf in q_files_no_requests), \
        '\n'.join('%s: %s' % (sf,sf.requests) \
                      for sf in q_files_no_requests)
    assert all(r.is_deletable for sf in purgable_files for r in sf.requests), \
        '\n'.join('%s: %s' % (sf,sf.requests) for sf in purgable_files)
    return purgable_files

def get_dispatchable_requests():
    return cel_db_session.query(DownloadRequest).\
        filter(DownloadRequest.state == 'created').\
        filter(~DownloadRequest.is_deletable).\
        order_by(DownloadRequest.time_created)

def get_files_offline_not_being_staged(r):
    return cel_db_session.query(StagableFile).\
        join(DownloadRequest.files).\
        filter(DownloadRequest.uuid == r.uuid).\
        filter(StagableFile.state == 'offline').\
        filter(StagableFile.staging_task == None)

def get_files_offline_being_staged(r):
    res = cel_db_session.query(StagableFile).\
        join(DownloadRequest.files).\
        filter(DownloadRequest.uuid == r.uuid).\
        filter(StagableFile.state == 'offline').\
        filter(StagableFile.staging_task != None)
    return res

# NOTE: due to the call to join_sizing_tasks this procedure might take
# a long time to complete.
#
# TODO: log error and resubmit itself at a later time on any exception
#       that occurs or use celery retry mechanism
@celery.task(acks_late=True, ignore_results=True)
def schedule_tasks():
    dispatchable_requests = get_dispatchable_requests()
    logger.info('running scheduler - %d dispatchable request(s): %s' % \
                ( dispatchable_requests.count(),
                  ', '.join(r.uuid for r in dispatchable_requests)))
    reserved_files = get_reserved_files()
    online_files = get_online_files()
    reserved_space = sum(sf.size for sf in reserved_files)
    used_space = sum(sf.size for sf in online_files)
    available_space = STAGE_SPACE - used_space
    purgable_files = get_purgable_files()
    purgable_amount = sum(sf.size for sf in purgable_files)

    assert len(reserved_files.all()) == len(set(reserved_files)), \
        reserved_files.all()
    assert used_space >= 0, used_space
    assert reserved_space >= 0, (reserved_files, reserved_space)
    assert purgable_amount <= STAGE_SPACE, (purgable_amount, STAGE_SPACE,
                                            reserved_space, available_space)
    logger.debug('reserved files: %s' % ', '.join(sf.name for sf in reserved_files))
    logger.debug('online files: %s' % ', '.join(sf.name for sf in online_files))
    logger.debug('purgable amount: %d bytes from %s' % \
                 (purgable_amount, ', '.join(sf.name for sf in purgable_files)))
    logger.info('total staging space: %d bytes, used: %d bytes, reserved: %d '
                'bytes. Max %d bytes available for new requests (%d bytes '
                'purgable)' % \
                (STAGE_SPACE, used_space, reserved_space,
                 STAGE_SPACE - reserved_space, purgable_amount))
    dispatch_tasks = True
    num_tasks_dispatched = 0
    num_tasks_failed = 0
    num_tasks_skipped = 0

    for rs in dispatchable_requests:
        try:
            assert available_space >= 0, (STAGE_SPACE, reserved_space,
                                          available_space)
            files_offline_not_being_staged = get_files_offline_not_being_staged(rs)
            logger.info('scheduling %s (waiting for all size estimation tasks),'
                        ' available space %d bytes, offline files: %s' % \
                        (rs.uuid, available_space,
                         ', '.join(sf.name for sf in files_offline_not_being_staged)))
            logger.debug('offline files: %s' % \
                         files_offline_not_being_staged.all())
            join_sizing_tasks(files_offline_not_being_staged.\
                                  filter(StagableFile.size == None))
        except Exception, e:
            # Note: all requests will keep on failing until this task has been resubmitted
            logger.warning('sizing estimation of %s failed: %s, request -> '
                           'failed' % (rs, e))
            fail_request(rs, 'sizing estimation of %s failed: %s' % (rs, e))
            num_tasks_failed += 1
            continue

        assert all(f.size is not None for f in files_offline_not_being_staged),\
            files_offline_not_being_staged
        offline_size = sum(f.size for f in files_offline_not_being_staged)
        total_size = sum(f.size for f in rs.files)
        logger.debug('total request size: %d bytes, %d offline bytes' % \
                     (total_size, offline_size))
        # if staging fails any later request will we must make sure
        # either to fail any later requests for that file

        # fastforward all dispatchable zero cost requests and any
        # requests that is impossible to fulfill:
        #    created -> failed,
        #    created -> dispatching,
        #    created -> finished:
        if total_size > STAGE_SPACE:
            logger.info('fast forwarding %s -> failed - since there is no way '
                         'it can be fulfilled (needs %d of %d bytes '
                         'available)' % \
                         (rs.uuid, total_size, STAGE_SPACE))
            fail_request(rs, '%s can not be fulfilled (needs %d of %d bytes '
                         'available)' % (rs.uuid, total_size, STAGE_SPACE))
            num_tasks_failed += 1
        elif not files_offline_not_being_staged.first():
            if get_files_offline_being_staged(rs).first():
                logger.info('fast forwarding %s -> dispatching - since it '
                             'fully overlaps with staging tasks in '
                             'progress' % rs.uuid)
                rs.state = 'dispatching'
                num_tasks_dispatched += 1
            else:
                # this should not fail unless db is being concurrently
                # updated:
                assert all(x.state == 'online' for x in rs.files), rs
                logger.info('fast forwarding %s -> finished - since all '
                            'files are online' % rs.uuid)
                finish_request(rs)
                num_tasks_dispatched += 1
        elif dispatch_tasks:
            if offline_size > available_space:
                logger.info('%s requires more space than what is available '
                            '(%d > %d), initiating cache purging' % \
                            ( rs, offline_size, available_space ))
                bytes_freed, requests_to_delete = purge_files(offline_size - available_space,
                                                              purgable_files)
                available_space += bytes_freed
                logger.info('freed up %d/%d bytes (%d%%) - available space %d '
                            'bytes' % \
                            ( bytes_freed, offline_size,
                              int(round(bytes_freed / float(offline_size) * 100), 3),
                              available_space ))
                if requests_to_delete:
                    assert all(x.is_deletable for x in requests_to_delete), requests_to_delete
                    logger.info('deleting all deletable requests from db '
                                'belonging to files that have been '
                                'purged: %s' % \
                                (x.uuid for x in requests_to_delete))
                    cel_db_session.query(DownloadRequest).\
                        filter(DownloadRequest.uuid.in_(x.uuid for x in requests_to_delete)).\
                        delete(synchronize_session='fetch')
                if offline_size > available_space:
                    logger.info('%d bytes needed but only %d bytes available -'
                                ' not considering any further requests for task'
                                ' dispatching until the next request has '
                                'finished/failed' % \
                                (offline_size, available_space))
                    dispatch_tasks = False
                    # schedule_tasks will be triggered upon next
                    # request failure or deletion after finish

            # optimisation possibility: we can also dispatch as many
            # tasks as there currently is room for even if the request
            # can not be fully staged yet (but that would also
            # complicate how we compute reserved/available_space)
            if dispatch_tasks:
                logger.info('staging offline files for request %s: %s' % \
                            (rs.uuid,
                             ', '.join(sf.name for sf in files_offline_not_being_staged)))
                for sf in files_offline_not_being_staged:
                    path_stdout = create_tempfile()
                    path_stderr = create_tempfile()
                    # we'd like to chain the
                    # schedule_join_staging_tasks here rather than
                    # calling it from the stage_file task, but then we
                    # cannot register the async result like this...
                    async_result = stage_file.delay(sf.name,
                                                    sf.path,
                                                    path_stdout,
                                                    path_stderr)
                    # note the critical window if worker dies here the
                    # task may have started but will not be registered
                    # with f (the same goes for submitting sizing
                    # tasks). We choose risking more than one
                    # identical staging task running (which is
                    # harmless and has a low probability, though
                    # wastes some resources) rather than risking
                    # having the task registered with the file but not
                    # actually running (which is harder to handle)
                    #
                    # (possibly create a celery task group consisting
                    # of staging -> checksumming):
                    sf.staging_task = Task(async_result.id,
                                           path_stdout,
                                           path_stderr)
                    # IMPORTANT: commit immediately since stage_file
                    # calls schedule_join_staging_tasks which looks up
                    # the task in the db:
                    cel_db_session.commit()
                    logger.info('=> staging task for %s is %s' % \
                                (sf.name, sf.staging_task.uuid))
                available_space -= offline_size
                rs.state = 'dispatched'
                num_tasks_dispatched += 1
        else:
            logger.info('request %s is not eliglible for dispatching since it '
                        'can not be fast forwarded and at least one higher '
                        'priority request is still waiting to be dispatched' % \
                        rs.uuid)
            num_tasks_skipped += 1

    logger.info('scheduling iteration completed, %d tasks dispatched, %d '
                'tasks failed, %d tasks skipped' % \
                (num_tasks_dispatched, num_tasks_failed, num_tasks_skipped))
    cel_db_session.commit()






### HTTP





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
def handle_api_exception(error):
    logger.debug(error)
    return ( jsonify(message=error.message, payload=error.payload),
             error.rc )

#@app.errorhandler(AssertionError)
#def handle_api_assertion_error(error):
#    raise APIException(500, str(error))

# Note: this won't be called if flask is running in debug mode
@app.errorhandler(500)
def handle_api_internal_error(error):
    logger.error(str(error))
    return ( jsonify(message='Internal Error', payload=str(error)), 500 )

@app.route('/request_demo', methods=['POST'])
def http_create_request_demo():
    logger.debug(request)
    r = select_request_input(request)
    openid = r.get('openid')
    if not openid:
        logger.warn('faulty request %s is missing attribute openid' % request)
        raise HTTPBadRequest('missing attribute openid (string)')
    assert type(openid) is str or type(openid) is unicode, openid
#    q = getEsgfQuery(r)
#    if not q:
#        logger.warn('faulty request %s is missing attribute query' % request)
#        raise HTTPBadRequest('missing attribute query (string) - ESGF style '
#                             'query', payload=str(request))
    # TODO: REMOVE ME
    file_names = r.get('files')
    if not file_names:
        logger.warn('faulty request %s is missing attribute files' % request)
        raise HTTPBadRequest('missing attribute files (list of strings)',
                             payload=str(request))
    assert type(file_names) is list, file_names
    assert all(type(x) is str or type(x) is unicode for x in file_names)

    q = { 'class'    : 'op',
          'stream'   : 'oper',
          'expver'   : 'c11a',
          'model'    : 'hirlam',
          'type'     : 'fc',
          'date'     : '20130601',
          'time'     : '00',
          'step'     : '0',
          'levtype'  : 'hl',
          'levelist' : '2',
          'param'    : '11.1' }
    file_to_query = {}
    for name in file_names:
        mars_params = ',\n'.join('%s = %s' % (k,v) for k,v in q.iteritems())
        file_to_query[name] = mars_params
    logger.debug('=> registering new request: openid=%s, file_to_query=%s' % \
                 (openid, file_to_query))

    # the reason we run this as a worker job (which only has one
    # worker) is to guarantee it is _not_ run concurrently. It makes
    # it less likely we get more than one identical staging/sizing
    # task running. We still can get orphan staging tasks if the
    # worker crasches in the wrong place - before it's been registered
    # in the sql db. But we don't risk each staging task being subject
    # to the race condition that occurs in the window between checking
    # for the existence of an existing task and starting a new task
    # (which can lead to > 1 identical tasks being executed in a
    # concurrent situation). This register_request task is
    # expected to be really quick so it's not expected to be a
    # performance issue.
    #
    # create request and submit sizing tasks for unknown file_names
    r_uuid = register_request_demo.delay(openid, file_to_query).get()
    logger.info('<= registered new request %s for openid=%s, file_to_query=%s' % \
                (r_uuid, openid, file_to_query))
    logger.debug('=> submitting sizing tasks for request id %s' % r_uuid)
    schedule_submit_sizing_tasks.delay(r_uuid)
    logger.debug('=> invoking scheduler')
    schedule_tasks.delay()
    return jsonify(), 201, { 'location': '/request/%s' % r_uuid }




# FIXME: requires authorization (used by the esg node)
# TODO: maybe limit the amount of retries, timeout?
@app.route('/request', methods=['POST'])
def http_create_request():
    logger.debug(request)
    r = select_request_input(request)
    openid = r.get('openid')
    if not openid:
        logger.warn('faulty request %s is missing attribute openid' % request)
        raise HTTPBadRequest('missing attribute openid (string)')
    assert type(openid) is str or type(openid) is unicode, openid
    # TODO: allow for file sizes to be injected ( files would be a
    # dict { file_name_1 { 'size' : size } }
    file_names = r.get('files')
    if not file_names:
        logger.warn('faulty request %s is missing attribute files' % request)
        raise HTTPBadRequest('missing attribute files (list of strings)',
                             payload=str(request))
    assert type(file_names) is list, file_names
    assert all(type(x) is str or type(x) is unicode for x in file_names)

    logger.debug('=> registering new request: openid=%s, file_names=%s' % \
                 (openid, file_names))

    # the reason we run this as a worker job (which only has one
    # worker) is to guarantee it is _not_ run concurrently. It makes
    # it less likely we get more than one identical staging/sizing
    # task running. We still can get orphan staging tasks if the
    # worker crasches in the wrong place - before it's been registered
    # in the sql db. But we don't risk each staging task being subject
    # to the race condition that occurs in the window between checking
    # for the existence of an existing task and starting a new task
    # (which can lead to > 1 identical tasks being executed in a
    # concurrent situation). This register_request task is
    # expected to be really quick so it's not expected to be a
    # performance issue.
    #
    # create request and submit sizing tasks for unknown file_names
    r_uuid = register_request.delay(openid, file_names).get()
    logger.info('<= registered new request %s for openid=%s, file_names=%s' % \
                (r_uuid, openid, file_names))
    logger.debug('=> submitting sizing tasks for request id %s' % r_uuid)
    schedule_submit_sizing_tasks.delay(r_uuid)
    logger.debug('=> invoking scheduler')
    schedule_tasks.delay()
    return jsonify(), 201, { 'location': '/request/%s' % r_uuid }


def select_request_input(request):
    content_type = request.headers['Content-Type']
    selector = { 'application/json' : request.json,
                 'application/x-www-form-urlencoded' : request.args }
    r = selector.get(content_type)
    if not r:
        raise HTTPBadRequest('Unsupported HTTP Content-Type %s - must be any '
                             'of %s' % \
                             (content_type, ', '.join(selector.keys())))
    return r

@app.route('/mars', methods=['POST'])
def mars():
    logger.debug(request)
    r = select_request_input(request)     # where r is a dict()
    # FIXME: sanitize this, don't let choose verb keywords, we also
    # _want_ to set target ourselves for each sub request
    verb = r.get('verb').upper()
    if not verb:
        raise HTTPBadRequest('missing verb (string) - LIST or RETRIEVE')
    params = dict((k.upper(),v) for k,v in r.get('params').iteritems())
    if not params:
        raise HTTPBadRequest('missing params (dict of string) - any valid MARS request keyword')
    logger.debug('request verb: %s' % verb)
    logger.debug('request params: %s' % params)
    if verb == 'RETRIEVE':
        fingerprint = verb + (','.join('%s=%s' % (k,v) for k,v in sorted(params.iteritems()) if k != 'target'))
        file_name = 'mars_%s.grb' % hashlib.sha1(fingerprint).hexdigest()
        logger.debug('file name: %s' % file_name)
        params['target'] = os.path.join(STAGEDIR, file_name)
    else:
        del params['target']
        raise AssertionError('TODO: LIST not yet implemented')
    mars_request = MarsRequest(verb, params)
    logger.debug('MARS request: %s' % mars_request)



# this would allow for the wget script to signal that all files have
# been downloaded, enabling space to be freed up as early as possible
@app.route('/request', methods=['DELETE'])
def http_delete_request():
    pass # TODO: only allow esg node or the request owner to delete a request


# FIXME: only to be called by matching openid
@app.route('/request/<uuid>', methods=['GET'])
def http_status_request(uuid):
    #assert request.headers['Content-Type'] == 'application/json', request
    #assert 'openid' in request.json, request.json
    r = DownloadRequest.query.get_or_404(uuid)
    all_files = set(r.files)
    staged = set(f for f in all_files if f.state == 'online')
    offline = all_files - staged
    progress = float(len(staged)) / len(r.files)
    is_done = len(offline) == 0
    urls_offline_files = [ url_for('http_status_file', uuid=r.uuid, file_name=x.name, _external=True) for x in offline ]
    urls_staged_files = [ url_for('http_serve_file', uuid=r.uuid, file_name=x.name, _external=True) for x in staged ]
    return jsonify(status=r.state,
                   progress=progress,
                   staged_files=urls_staged_files,
                   offline_files=urls_offline_files), 200, { 'location' : '/request/%s' % r.uuid }

@app.route('/request/<uuid>/status/<file_name>', methods=['GET'])
def http_status_file(uuid, file_name):
    r = DownloadRequest.query.get_or_404(uuid)
    # FIXME: query r for files in r directly
    f = StagableFile.query.get_or_404(file_name)
    assert r in f.requests, (r, f)
    # TODO: if is done do redirect to staged? or provide link?
    return jsonify(stdout=f.staging_task.stdout(), stderr=f.staging_task.stderr()), 200



# set time of cache expiry?
# use some reference counting scheme? but we cannot reliably detect
# successful downloads? we'd also avoid derefence twice for the same
# file and request

# possibly use http content disposition?
@app.route('/request/<uuid>/staged/<file_name>', methods=['GET'])
def http_serve_file(uuid, file_name):
    f = StagableFile.query.get_or_404(file_name)
    f.time_accessed = func.now()
    db.session.commit()
    return redirect(url_for('http_render_static', uuid=uuid, file_name=file_name))

# FIXME: REMOVEME: should be served from apache or whatever will be
#        running in production
# FIXME: implement authorization - you're not allowed to download any
#        files just like that even if they happen to be available.
# X-Sendfile
# http://www.yiiframework.com/wiki/129/x-sendfile-serve-large-static-files-efficiently-from-web-applications/
# http://pythonhosted.org/xsendfile/
@app.route('/static/<uuid>/staged/<file_name>', methods=['GET'])
def http_render_static(uuid, file_name):
    r = DownloadRequest.query.get_or_404(uuid)
    f = StagableFile.query.get_or_404(file_name)
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



def exec_proc(cmd, stdin=None, term_timeout=5):

    assert type(cmd) is list, 'cmd %s' % cmd
    assert all(type(x) is str for x in cmd), 'cmd %s' % cmd
    assert stdin is None or type(stdin) is str or type(stdin) is unicode, 'stdin %s' % type(stdin)
    assert term_timeout >= 0, 'term_timeout %s' % term_timeout

    # FIXME: use os.devnull if stdin/stdout/stderr is unused, and
    # remember to close it when returning

    logger.debug('executing %s' % cmd)

    LINE_BUFFERED = 1
    p = subprocess.Popen(cmd,
                         shell=False,
                         bufsize=LINE_BUFFERED,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    try:
        logger.debug('pid is %d' % p.pid)
        # FIXME: split into digestable chunks and use select,
        # otherwise we may deadlock, but should be alright for <= 4096
        # bytes:
        if stdin is not None:
            p.stdin.write(stdin)
            p.stdin.flush()
            p.stdin.close()
        for fd, data in fd_read_outerr(p.stdout, p.stderr):
            # FIXME: merge data not containing newline into lines
            for l in data.splitlines(True):
                fileno = 1 if fd == p.stdout else 2
                yield None, fileno, l
        rc = p.wait()
    except Exception, e:
        logger.error(e)
        logger.warn('terminating process %s' % p)
        # FIXME: make sure killing this MARS process also kills the
        # underlying tunnel:
        p.terminate()
        try:
            rc = timed_wait(p, term_timeout)
        except TimeoutException:
            logger.warn('timed out (after %d s.) waiting for process %s to '
                        'terminate - sending SIGKILL' % ( term_timeout, p ))
            p.kill()
            rc = p.wait()

    yield rc, None, None

def unlink(file_name):
    try:
        os.unlink(file_name)
        return True
    except OSError, e:
        if e.errno != errno.ENOENT:
            raise
        return False


#    params = { 'class'    : 'op',
#               'stream'   : 'oper',
#               'expver'   : 'c11a',
#               'model'    : 'hirlam',
#               'type'     : 'fc',
#               'date'     : '20130601',
#               'time'     : '00',
#               'step'     : '0',
#               'levtype'  : 'hl',
#               'levelist' : '2',
#               'param'    : '11.1' }
def create_mars_request(verb, file_name, target=None):
    sf = cel_db_session.query(StagableFile).get(file_name)
    logger.debug('creating MARS request from %s' % sf)
    params = {}
    for l in sf.params.split(',\n'):
        k,v = l.split('=', 1)
        params[k.strip()] = v.strip()
    if target:
        params['target'] = target
    req = MarsRequest(verb, params)
    logger.debug('MARS request is: %s' % req)
    return req


class MarsRequest(object):

    _allowed_verbs = [ 'LIST', 'RETRIEVE' ]

    def __init__(self, verb, params):
        assert isinstance(params, dict), params
        assert all(type(x) is str or type(x) is unicode for x in params.keys()), params
        assert all(type(x) is str or type(x) is unicode for x in params.values()), params
        if verb.upper() not in MarsRequest._allowed_verbs:
            raise Exception('illegal MARS request verb: %s - please use any '
                            'of: %s' % ( verb, MarsRequest._allowed_verbs ))
        param_target = params.get('target')
        if param_target:
            target = os.path.normpath(param_target)
            if target.startswith(STAGEDIR + '/'):
                params['target'] = "'%s'" % target
            else:
                raise Exception('invalid path: %s - must be below %s' % \
                                (target, STAGEDIR))
        self.verb = verb
        self.params = params

    def __str__(self):
        params = ',\n'.join('    %s=%s' % (k,v) for k,v in self.params.iteritems())
        return "%s,\n%s" % (self.verb, params)


# FIXME: one notable failure case to handle is if unexpectedly
# running out of staging space
# FIXME: implement retry mechanism
@celery.task(acks_late=True)
def stage_file(file_name, target_dir, path_out, path_err):
    logger.info('staging %s' % (os.path.join(target_dir, file_name)))
    try:
        tmp_target = os.path.join(target_dir, uuid.uuid4().get_hex())
        logger.debug('tmp_target: %s' % tmp_target)
        mars_request = create_mars_request(verb='RETRIEVE',
                                           file_name=file_name,
                                           target=tmp_target)
        logger.debug('mars_request: %s' % mars_request)

        with open(path_out, 'w') as f_out:
            with open(path_err, 'w') as f_err:
                for rc,fd,l in exec_proc([ 'mars' ], stdin=str(mars_request)):
                    if fd is not None and l is not None:
                        if fd == 1:
                            logger.debug('fd=%s, l=%s' % (fd, l.strip() if l else l))
                        else:
                            logger.warning('fd=%s, l=%s' % (fd, l.strip() if l else l))
                        f = f_out if fd == 1 else f_err
                        f.write(l)
                        f.flush()
                for f in [f_out, f_err]:
                    f.flush()
                    os.fdatasync(f.fileno())
                    f.close()
        if rc != 0:
            logger.debug('removing temp file %s' % tmp_target)
            unlink(tmp_target) # FIXME: use try...finally
            raise TaskFailure('mars returned %d' % rc)

        end_target = os.path.join(target_dir, file_name)
        logger.debug('moving temp file %s -> %s' % (tmp_target, end_target))
        os.rename(tmp_target, end_target)
        logger.info('%s is staged online' % end_target)
    finally:
        logger.debug('=> invoking scheduler')
        schedule_join_staging_task.delay(stage_file.request.id)

@celery.task(acks_late=True)
def checksum_file(file_path):
    with open(file_path) as f:
        digest = hashlib.sha1()
        while True:
            bytes = f.read(8 * 1024)
            if bytes == "":
                break
            digest.update(bytes)
        return digest.hexdigest()


# parse the size from mars list output:
# ...
# Grand Total:
# ============
#
# Entries       : 1,008
# Total         : 204,381,458 (194.913 Mbytes)
#
# consider resubmitting itself a certain amount of times and after
# that it should fail the request
@celery.task(acks_late=True)
def estimate_size(file_name):
    logger.info('calling MARS to estimate size for %s' % file_name)
    size = None
    re_size = re.compile(r'^Total +: *([\d,]+) +\(.+\)$')
    mars_request = create_mars_request(verb='LIST', file_name=file_name)
    logger.debug('mars_request: %s' % mars_request)

    for rc,fd,l in exec_proc([ 'mars' ], stdin=str(mars_request)):
        logger.debug('rc = %s, fd = %s, l = %s' % (rc, fd, l.strip() if l else l))
        if size is None and fd == 1:
            m = re_size.match(l)
            if m:
                logger.debug('got match %s' % m.group(1))
                size = int(m.group(1).replace(",", ""))
            else:
                logger.debug('no match')
    assert rc is not None and fd is None and l is None
    # don't trust size if mars returns non-zero
    if rc != 0 or size is None:
        logger.error('failed to compute size, rc = %d' % rc)
        raise TaskFailure('failed to compute size, rc = %d' % rc)
    est_size = size * FILE_SIZE_WEIGHT + FILE_SIZE_EXTRA
    logger.debug('MARS reported size: %d bytes, after compensating: size * %d + %d = %d' % \
                 (size, FILE_SIZE_WEIGHT, FILE_SIZE_EXTRA, est_size))
    logger.info('size is %d' % est_size)
    return est_size


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
