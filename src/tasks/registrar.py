import celery
import tasks
from models import DownloadRequest, StagableFile, Task
from tasks import logger


@celery.task(acks_late=True)
def register_request_demo(openid, file_to_query):
    r = DownloadRequest(openid)
    logger.debug('created new request %s' % r)
    file_names = file_to_query.keys()
    registered_files = tasks.session.query(StagableFile).\
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
    tasks.session.add(r)
    tasks.session.commit()
    logger.info('registered request %s for openid=%s, file_names=%s '
                '(unregistered=%s)' % \
                (r.uuid, openid, file_names, new_file_names))
    return r.uuid


@celery.task(acks_late=True)
def register_request(openid, file_names):
    r = DownloadRequest(openid)
    logger.debug('created new request %s' % r)
    registered_files = tasks.session.query(StagableFile).\
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
    tasks.session.add(r)
    tasks.session.commit()
    logger.info('registered request %s for openid=%s, file_names=%s '
                '(unregistered=%s)' % \
                (r.uuid, openid, file_names, new_file_names))
    return r.uuid
