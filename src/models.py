from __future__ import absolute_import
import config
import os
import tasks
import uuid
from datetime import datetime
from flask.ext.sqlalchemy import SQLAlchemy
from sqlalchemy import CheckConstraint, or_


db = SQLAlchemy()

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

    def __init__(self, uuid, openid):
        self.uuid = uuid
        self.openid = openid
        self.time_created = datetime.utcnow()

    def __repr__(self):
        return ( '%s(uuid=%s, openid=%s, time_created=%s, state=%s, '
                 'is_deletable=%s)' % \
                 ( type(self).__name__, self.uuid, self.openid,
                   self.time_created, self.state, self.is_deletable ))


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
    __table_args__ = ( CheckConstraint(request_count >= 0),
                       CheckConstraint(or_(size == None, size >= 0)) )

    def __init__(self, name, params=None, size=None, checksum_sha1=None, request_count=1,
                 path=config.STAGEDIR):
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
    path_out = db.Column(db.Text)

    def __init__(self, uuid, path_out=None):
        self.uuid = uuid
        self.path_out = path_out

    def __repr__(self):
        return '%s(uuid=%s, status=%s, path_out=%s)' % \
            ( type(self).__name__,
              self.uuid,
              self.future().status,
              self.path_out )

    def get(self):
        return self.future().get()

    def future(self):
        return tasks.cel.AsyncResult(self.uuid)

    # we can't use terminate=True to kill the task since this may kill
    # the wrong task if the intended receipient finishes before being
    # terminated, so this will just revoke not already started tasks
    #
    # TODO: find out whether we can terminate by using amqp
    # or we register the pid of the mars process and kill that (assuming we can deliver this to the same worker
    def cancel(self):
        self.future().revoke()

    def is_done(self):
        return self.future().ready()

    def is_failed(self):
        return self.future().ready() and self.future().status != 'SUCCESS'

    def output(self):
        with open(self.path_out) as f:
            return f.read()
