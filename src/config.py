from __future__ import absolute_import
import kombu
import os
from util import util


# log to syslog: http://www.toforge.com/2011/06/celery-centralized-logging/
LOGFORMAT = '%(asctime)s %(levelname)-7s %(message)s'

#CELERY_RESULT_ENGINE_OPTIONS = { 'echo': True }

BROKER_URL = 'amqp://guest@localhost//'
CELERY_RESULT_BACKEND = 'amqp://guest@localhost//'
CELERYD_TASK_LOG_FORMAT = LOGFORMAT
CELERYD_LOG_FORMAT = '%(asctime)s %(levelname)-7s <celery> %(message)s'
# explicitly accept pickle (which we need to be able to serialize
# python exceptions etc.) to get rid of security warnings at startup):
CELERY_ACCEPT_CONTENT = [ 'pickle', 'json', 'msgpack', 'yaml' ]
CELERY_QUEUES = (
    kombu.Queue('default',
                kombu.Exchange('default'),
                routing_key='default'),    # >= 1 worker
    kombu.Queue('scheduler',
                kombu.Exchange('default'),
                routing_key='schedule')    # == 1 worker
    )
CELERY_ROUTES = { 'tasks.scheduler.register_request' : { 'queue' : 'scheduler' },
                  'tasks.scheduler.schedule_tasks' : { 'queue' : 'scheduler' },
                  'tasks.scheduler.schedule_join_staging_task' : { 'queue' : 'scheduler' },
                  'tasks.scheduler.schedule_mark_request_deletable' : { 'queue' : 'scheduler' } }

CELERY_DEFAULT_QUEUE = 'default'
CELERY_DEFAULT_EXCHANGE_TYPE = 'direct'
CELERY_DEFAULT_ROUTING_KEY = 'default'


SQLALCHEMY_DATABASE_URI = 'sqlite:////tmp/flask.db'

STAGEDIR = os.path.realpath(os.getenv('STAGEDIR', os.getenv('TMPDIR', '/tmp')))
# Assumes we own the whole filesystem. Possibly we should add a way to
# reserve a percentage or static amount of STAGE_SPACE rather than use
# all of it:
STAGE_SPACE = int(util.df(STAGEDIR)['1-blocks'])

# How long a request should exist in the db after it is finished or
# failed (in seconds). Only finished requests will keep reserving file
# space until deleted. Any files solely belonging to failed or
# deletable requests will be eligible for purging.
#REQUEST_PINNING_TIME = 24 * 3600
REQUEST_PINNING_TIME = 60
FILE_SIZE_EXTRA = 256
FILE_SIZE_WEIGHT = 1
