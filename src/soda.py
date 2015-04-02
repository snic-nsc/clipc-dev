from celery import Celery
from celery.signals import after_task_publish
from datetime import datetime
from flask import Flask, jsonify, request, redirect, url_for, send_from_directory
from flask.ext.sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func
import hashlib
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

app = Flask('soda')
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/flask.db'
db = SQLAlchemy(app)

CELERY_TASK_DB = './celery_task.db'
# TODO: CELERY_ACCEPT_CONTENT = [ 'pickle', 'json', 'msgpack', 'yaml' ]

cel = Celery(app.name,
             backend='db+sqlite:///' + CELERY_TASK_DB,
             broker='amqp://guest@localhost//')
cel.conf.update(app.config)

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
STAGEDIR = TMPDIR

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

    # maybe we should include the full path into the db? otherwise we
    # have a side effect if TMPDIR is changed
    def is_staged(self):
        return os.path.exists(self.path_staged())

    def is_staging(self):
        return self.task and self.task.is_valid()



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



# FIXME: requires authorization (used by the esg node)
# TODO: maybe limit the amount of retries, timeout?
@app.route('/new_request', methods=['POST'])
def render_new_request():
    assert request.headers['Content-Type'] == 'application/json', request
    assert 'openid' in request.json, request.json
    openid = request.json['openid']
    files = request.json['files']
    while True:
        try:
            r = DownloadRequest(openid)
            print 'got new request %s' % r

            for file_name in files:
                pf = PubFile.query.get(file_name)
                if pf:
                    # FIXME: verify this doesn't update twice in event of IntegrityError
                    pf.count_downloads = PubFile.count_downloads + 1
                    print '%s exists in db' % pf
                else:
                    pf = PubFile(file_name)
                    print '%s does not exist in db' % pf
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
                # TODO: use python logging
                print 'got %s, rolling back and retrying...' % str(e)
                db.session.rollback()
            else:
                raise

    print r
    print r.files

    # Note: r is r still valid after the commit

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

    print 'staged files: %s' % staged_files
    print 'staging files: %s' % [ (f, f.task, f.task.future().status) for f in staging_files ]
    print 'unstaged files: %s' % unstaged_files

    # FIXME: handle ioexception or write into user writable dir only,
    # celery_uuid.{out,err}
    for pf in unstaged_files:
        _, path_stdout = tempfile.mkstemp()
        _, path_stderr = tempfile.mkstemp()
        delay = 1
        count = 60
        # race condition: there's a risk of more than one concurrent identical request here
        async_result = stage_file.delay(pf.path_staged(), path_stdout, path_stderr, delay, count)
        print 'executed task %s' % async_result
        task = Task(async_result.id, path_stdout, path_stderr)
        pf.task = task
        print 'created task %s in db' % task

        #TODO: test with time.sleep(10) and concurrent request for same files when having a concurrent web server

    db.session.commit() # or internal error

#    orphans = Task.query.filter(Task.pub_file is None)
#    for t in orphans:
#        # there's a risk this misses and kills the wrong task if a new one gets to start
#        print 'killing orphan task %s' % t
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
    return f.task.stdout(), 200, { 'Content-Type' : 'text/plain' }


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
            isEOF = len(data) == 0
            if isEOF:
                rlist.remove(fd)
            else:
                yield fd, data




@cel.task(acks_late=True)
def stage_file(result_file, path_out, path_err, delay, count):

    # task_id = stage_file.request.id

    if os.path.exists(result_file):
        return 0

    LINE_BUFFERED = 1
    p = subprocess.Popen(['vmstat', str(delay), str(count)],
                         shell=False,
                         bufsize=LINE_BUFFERED,
                         stdin=subprocess.PIPE, # or maybe not set this and instead set close_fds=True?
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    try:
        with open(path_out, 'w') as f_out:
            with open(path_err, 'w') as f_err:
                # we could consider terminating if we detect that the file exists (but we don't want to terminate both)
                # we could consider terminating if we detect there's another task running (but we don't want to terminate both)
                # we could consider terminating if we detect that this task is an orphan
                for fd, data in fd_read_outerr(p.stdout, p.stderr):
                    f = f_out if fd == p.stdout else f_err
                    f.write(data)
                    # FIXME: only flush every certain interval (make configurable)
                    f.flush()
                f_err.flush()
                os.fdatasync(f_err.fileno())
            f_out.flush()
            os.fdatasync(f_out.fileno())
    except Exception, e:
        # FIXME: p.terminate()
        print e
    finally:
        rc = p.wait()
        if rc == 0:
            try:
                # FIXME: must be atomic
                with open(result_file, 'w') as f:
                    f.write('done\n')
                    f.flush()
                    os.fdatasync(f.fileno())
            except:
                # FIXME: log errors
                rc = 1
        return rc

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
