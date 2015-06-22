from __future__ import absolute_import
import celery
import celery.signals
import config
import logging
from celery.utils.log import get_task_logger
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


@celery.signals.worker_init.connect
def establish_db_session(signal, sender):
    global session
    engine = create_engine(config.SQLALCHEMY_DATABASE_URI, echo=False)
    session = scoped_session(sessionmaker(autocommit=False,
                                                 autoflush=True,
                                                 bind=engine))


@celery.signals.task_postrun.connect
def remove_db_session(**kwargs):
    session.remove()


cel = celery.Celery('soda',
                    include=['tasks.scheduler',
                             'tasks.worker'])
cel.config_from_object(config)
logger = get_task_logger('soda')
logger.setLevel(logging.INFO)
