from __future__ import absolute_import
import celery
import config
import logging
from celery.utils.log import get_task_logger
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


@celery.signals.worker_init.connect
def establish_db_session(signal, sender):
    global cel_db_session
    engine = create_engine(config.SQLALCHEMY_DATABASE_URI, echo=False)
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


cel = celery.Celery('soda',
                    include=['tasks.registrar',
                             'tasks.scheduler',
                             'tasks.worker'])
cel.config_from_object(config)
logger = get_task_logger('soda')
logger.setLevel(logging.INFO)
