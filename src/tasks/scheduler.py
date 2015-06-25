from __future__ import absolute_import
import celery
import config
import os
import tasks
import time
from celery.result import allow_join_result
from tasks.exceptions import TaskFailure
from models import DownloadRequest, StagableFile, Task
from sqlalchemy import or_
from sqlalchemy.sql import func
from tasks import logger
from util import util


@tasks.cel.task(bind=True, acks_late=True, ignore_results=True)
def register_request(self, uuid, openid, file_to_query):
    try:
        r = DownloadRequest(uuid, openid)
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
        logger.info('registered request %s openid=%s, file_names=%s '
                    '(unregistered=%s)' % \
                    (r.uuid, openid, file_names, new_file_names))
        submit_sizing_tasks(r.uuid)
        logger.debug('=> invoking scheduler')
        schedule_tasks.delay()
    except Exception, e:
        # Note that some errors will never complete OK, for example if
        # we try to register two or more requests with the same uuid
        # (unlikely) - these cases are not handled and will need
        # manual admin intervention (in this case removal of the
        # redundant requests)
        logger.error('register_request(uuid=%s, openid=%s, file_to_query=%s) '
                     'unexpectedly failed (task id %s): %s, retrying in 60 s...' %
                     (uuid, openid, file_to_query, register_request.request.id,
                      str(e)))
        raise self.retry(exc=e, countdown=60)


# raises OSError if failing to delete a file except when it does not
# exist updates db
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
                util.unlink(sf.staging_task.path_out)
            is_deleted = util.unlink(sf.path_staged())
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
def submit_sizing_tasks(r_uuid):
    files_unknown_size = tasks.session.query(StagableFile).\
        join(StagableFile.requests).\
        filter(DownloadRequest.uuid == r_uuid).\
        filter(StagableFile.size == None).\
        filter(StagableFile.sizing_task == None)
    logger.info('=> submitting sizing tasks for request %s: %s' % \
                (r_uuid, ', '.join(sf.name for sf in files_unknown_size)))
    for sf in files_unknown_size:
        async_result = tasks.worker.estimate_size.delay(sf.name)
        sf.sizing_task = Task(async_result.id)
        logger.debug('submitted: %s' % sf.sizing_task)
    tasks.session.commit()


# wait for the result of all sizing tasks, update the db with the
# result and delete the sizing task entry from the db. Requires a
# commit when done. Rraises any exception that occurred in the task
def join_sizing_tasks(files):
    with allow_join_result():
        for sf in files:
            try:
                assert sf.size is None, sf
                logger.debug('<= awaiting size estimation result for %s, '
                             'task is %s' % (sf, sf.sizing_task))
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
                tasks.session.delete(sf.sizing_task)
                sf.sizing_task = None


@tasks.cel.task(bind=True, acks_late=True, ignore_results=True)
def mark_request_deletable(self, r_uuid):
    try:
        r = tasks.session.query(DownloadRequest).get(r_uuid)
        logger.info('marking %s request %s as deletable' % (r.state, r_uuid))
        r.is_deletable = True
        tasks.session.commit()
        # since there might be pending requests that wait for space to
        # be freed up:
        logger.debug('=> invoking scheduler')
        schedule_tasks.delay()
    except Exception, e:
        logger.error('mark_request_deletable(uuid=%s) unexpectedly failed '
                     '(task id %s): %s, retrying in 60 s...' %
                     (r_uuid, mark_request_deletable.request.id, str(e)))
        raise self.retry(exc=e, countdown=60)


def notify_user(r, msg=None):
    pass
    #logger.error('TODO: NOT IMPLEMENTED: notify_user')


def finish_request(r):
    r.state = 'finished'
    notify_user(r, 'Request %s finished OK' % r)
    mark_request_deletable.apply_async(args=[r.uuid],
                                       countdown=config.REQUEST_PINNING_TIME)


def staging_tasks_owned_by(r):
    res = tasks.session.query(Task).\
        join(StagableFile.staging_task).\
        join(StagableFile.requests).\
        filter(~StagableFile.requests.any(DownloadRequest.uuid != r.uuid))
    assert all(x.uuid == r.uuid for t in res for x in t.requests), (r, res)
    return res


def fail_request(r, msg):
    r.state = 'failed'
    for t in staging_tasks_owned_by(r):
        logger.debug('revoking task %s belonging only to failed request %s' % \
                     (t, r))
        t.cancel()
    notify_user(r, msg)
    mark_request_deletable.apply_async(args=[r.uuid],
                                       countdown=config.REQUEST_PINNING_TIME)


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
    res = tasks.session.query(DownloadRequest).\
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
    size_ratio_allowed = 1
    size_diff = sf.size - real_size
    size_ratio = sf.size / float(real_size)
    if size_ratio > size_ratio_allowed:
        logger.warning('reported file size %d and actual file size %d '
                       'differs with more than %.1f%%' % \
                       (sf.size, real_size, 100 * size_ratio - 100))
    if sf.size != real_size:
        logger.info('updating db file size %d -> %d, (diff = %d bytes, '
                    'ratio = %f)' % \
                    (sf.size, real_size, size_diff, size_ratio))
        sf.size = real_size


@tasks.cel.task(acks_late=True, ignore_results=True)
def join_staging_task(task_id):
    # the task entry might not have been commited into the db yet
    while True:
        staging_task = tasks.session.query(Task).get(task_id)
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
            tasks.session.delete(sf.staging_task)
            util.unlink(sf.staging_task.path_out)
            sf.staging_task = None
            assert sf.staging_task is None, sf
            logger.debug('db commit')
            tasks.session.commit() # verify that we really need this - but it's important that the query below includes r
            for r in finishable_requests(sf):
                logger.info('request %s finished' % r.uuid)
                finish_request(r)
    except Exception, e: # fixme: catch some, reraise others
        logger.warning('%s failed: %s' % (staging_task, e))
        # consider resubmitting the task a limited amount since it
        # should only fail in rare cases
        logger.debug('deregistering %s from %s' % (sf.staging_task, sf))
        tasks.session.delete(staging_task)
        sf.staging_task = None
        assert sf.staging_task is None, sf
        logger.debug('db commit')
        tasks.session.commit()
        for r in dispatched_requests(sf):
            logger.info('request %s failed' % r.uuid)
            fail_request(r, 'Staging of %s failed: %s' % (sf, str(e)))
    finally:
        tasks.session.commit()


def get_online_files():
    return tasks.session.query(StagableFile).\
        filter(StagableFile.state == 'online')


def get_reserved_files():
    return tasks.session.query(StagableFile).\
        join(StagableFile.requests).\
        filter(~DownloadRequest.is_deletable).\
        filter(DownloadRequest.state.in_([ 'dispatched', 'finished' ]))


# Note: an active (dispatched/finished) request will keep reserving
# file space until explicitly being removed or marked as deletable in
# the db. A failed request will be present in the db until deleted,
# but its files won't be reserved.
def get_reserved_space():
    reserved_space = tasks.session.query(func.sum(StagableFile.size)).\
        join(StagableFile.requests).\
        filter(~DownloadRequest.is_deletable).\
        filter(DownloadRequest.state.in_([ 'dispatched', 'finished' ])).\
        scalar()
    if reserved_space is not None:
        assert reserved_space == sum(sf.size for sf in get_reserved_files())
        return reserved_space
    return 0


# return all StagableFile where either it is an orphan belonging to no
# request, or all its requests are either deletable or failed
def get_purgable_files():
    q_files_all_requests_failed_or_deletable = tasks.session.query(StagableFile).\
        join(StagableFile.requests).\
        filter(StagableFile.state == 'online').\
        filter(~StagableFile.requests.any(or_(~DownloadRequest.state == 'failed',
                                               ~DownloadRequest.is_deletable)))
    q_files_no_requests = tasks.session.query(StagableFile).\
        filter(StagableFile.state == 'online').\
        filter(~StagableFile.requests.any())
    purgable_files = q_files_all_requests_failed_or_deletable.\
        union(q_files_no_requests).order_by(StagableFile.request_count) # or order by the current request reference count

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
    return tasks.session.query(DownloadRequest).\
        filter(DownloadRequest.state == 'created').\
        filter(~DownloadRequest.is_deletable).\
        order_by(DownloadRequest.time_created)


def get_files_offline_not_being_staged(r):
    return tasks.session.query(StagableFile).\
        join(DownloadRequest.files).\
        filter(DownloadRequest.uuid == r.uuid).\
        filter(StagableFile.state == 'offline').\
        filter(StagableFile.staging_task == None)


def get_files_offline_being_staged(r):
    res = tasks.session.query(StagableFile).\
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
@tasks.cel.task(acks_late=True, ignore_results=True)
def schedule_tasks():
    dispatchable_requests = get_dispatchable_requests()
    logger.info('running scheduler - %d dispatchable request(s): %s' % \
                ( dispatchable_requests.count(),
                  ', '.join(r.uuid for r in dispatchable_requests)))
    reserved_files = get_reserved_files()
    online_files = get_online_files()
    reserved_space = sum(sf.size for sf in reserved_files)
    used_space = sum(sf.size for sf in online_files)
    available_space = config.STAGE_SPACE - used_space
    purgable_files = get_purgable_files()
    purgable_amount = sum(sf.size for sf in purgable_files)

    assert len(reserved_files.all()) == len(set(reserved_files)), \
        reserved_files.all()
    assert used_space >= 0, used_space
    assert reserved_space >= 0, (reserved_files, reserved_space)
    logger.debug('reserved files: %s' % ', '.join(sf.name for sf in reserved_files))
    logger.debug('online files: %s' % ', '.join(sf.name for sf in online_files))
    logger.debug('purgable amount: %d bytes from %s' % \
                 (purgable_amount, ', '.join(sf.name for sf in purgable_files)))
    logger.info('total staging space: %d bytes, used: %d bytes, reserved: %d '
                'bytes. Max %d bytes available for new requests (%d bytes '
                'purgable)' % \
                (config.STAGE_SPACE, used_space, reserved_space,
                 config.STAGE_SPACE - reserved_space, purgable_amount))
    dispatch_tasks = True
    num_tasks_dispatched = 0
    num_tasks_failed = 0
    num_tasks_deferred = 0
    consumed_space = 0

    for rs in dispatchable_requests:
        try:
            files_offline_not_being_staged = get_files_offline_not_being_staged(rs)
            logger.info('scheduling %s, available space %d bytes, offline files: %s' % \
                        (rs.uuid, available_space,
                         ', '.join(sf.name for sf in files_offline_not_being_staged)))
            logger.debug('offline files: %s' % \
                         files_offline_not_being_staged.all())
            logger.info('waiting for all size estimation tasks...')
            join_sizing_tasks(files_offline_not_being_staged.\
                                  filter(StagableFile.size == None))
        except Exception, e:
            # Note: all requests will keep on failing until this task has been resubmitted
            logger.warning('sizing estimation of %s failed: %s, request -> '
                           'failed' % (rs.uuid, e))
            fail_request(rs, 'sizing estimation of %s failed: %s' % (rs.uuid, e))
            num_tasks_failed += 1
            continue

        assert all(f.size is not None for f in files_offline_not_being_staged),\
            files_offline_not_being_staged
        offline_size = sum(f.size for f in files_offline_not_being_staged)
        total_size = sum(f.size for f in rs.files)
        logger.info('total request size: %d bytes, %d offline bytes' % \
                    (total_size, offline_size))
        # if staging fails any later request will we must make sure
        # either to fail any later requests for that file

        # fastforward all dispatchable zero cost requests and any
        # requests that is impossible to fulfill:
        #    created -> failed,
        #    created -> dispatched,
        #    created -> finished:
        if total_size > config.STAGE_SPACE:
            logger.info('fast forwarding %s -> failed - since there is no way '
                         'it can be fulfilled (needs %d of %d bytes '
                         'available)' % \
                         (rs.uuid, total_size, config.STAGE_SPACE))
            fail_request(rs, '%s can not be fulfilled (needs %d of %d bytes '
                         'available)' % (rs.uuid, total_size, config.STAGE_SPACE))
            num_tasks_failed += 1
        elif not files_offline_not_being_staged.first():
            if get_files_offline_being_staged(rs).first():
                logger.info('fast forwarding %s -> dispatched - since it '
                             'fully overlaps with staging tasks in '
                             'progress' % rs.uuid)
                rs.state = 'dispatched'
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
                            ( rs.uuid, offline_size, available_space ))
                required_amount = offline_size - available_space
                bytes_freed, requests_to_delete = purge_files(required_amount,
                                                              purgable_files)
                available_space += bytes_freed
                logger.info('freed up %d/%d bytes (%d%%) - available space %d '
                            'bytes' % \
                            ( bytes_freed, required_amount,
                              int(round(bytes_freed / float(offline_size) * 100, 3)),
                              available_space ))
                if requests_to_delete:
                    assert all(x.is_deletable for x in requests_to_delete), requests_to_delete
                    logger.info('deleting all deletable requests from db '
                                'belonging to files that have been '
                                'purged: %s' % \
                                ', '.join(x.uuid for x in requests_to_delete))
                    tasks.session.query(DownloadRequest).\
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
                    num_tasks_deferred += 1

            # optimisation possibility: we can also dispatch as many
            # tasks as there currently is room for even if the request
            # can not be fully staged yet (but that would also
            # complicate how we compute reserved/available_space)
            if dispatch_tasks:
                logger.info('staging offline files for request %s: %s' % \
                            (rs.uuid,
                             ', '.join(sf.name for sf in files_offline_not_being_staged)))
                for sf in files_offline_not_being_staged:
                    path_out = util.create_tempfile()
                    # we'd like to chain the join_staging_tasks here
                    # rather than calling it from the stage_file task,
                    # but then we cannot register the async result
                    # like this...
                    async_result = tasks.worker.stage_file.delay(sf.name,
                                                                 sf.path,
                                                                 path_out)
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
                    sf.staging_task = Task(async_result.id, path_out)
                    # IMPORTANT: commit immediately since stage_file
                    # calls join_staging_tasks which looks up the task
                    # in the db:
                    tasks.session.commit()
                    logger.info('=> staging task for %s is %s' % \
                                (sf.name, sf.staging_task.uuid))
                consumed_space += offline_size
                available_space -= offline_size
                rs.state = 'dispatched'
                num_tasks_dispatched += 1
        else:
            logger.info('request %s is not eliglible for dispatching since it '
                        'can not be fast forwarded and at least one higher '
                        'priority request is still waiting to be dispatched' % \
                        rs.uuid)
            num_tasks_deferred += 1

    logger.info('scheduling iteration completed, %d tasks dispatched, %d '
                'tasks failed, %d tasks deferred. Consumed space %d bytes, '
                'total space available %d bytes' % \
                (num_tasks_dispatched, num_tasks_failed, num_tasks_deferred,
                 consumed_space, available_space))
    tasks.session.commit()
