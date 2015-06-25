from __future__ import absolute_import
import celery
import config
import hashlib
import os
import re
import tasks
import uuid
from models import DownloadRequest, StagableFile, Task
from tasks import logger
from tasks.exceptions import TaskFailure, HardTaskFailure
from util import util


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
            if target.startswith(config.STAGEDIR + '/'):
                params['target'] = "'%s'" % target
            else:
                raise Exception('invalid path: %s - must be below %s' % \
                                (target, config.STAGEDIR))
        self.verb = verb
        self.params = params

    def __str__(self):
        params = ',\n'.join('    %s=%s' % (k,v) for k,v in self.params.iteritems())
        return "%s,\n%s" % (self.verb, params)


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
    sf = tasks.session.query(StagableFile).get(file_name)
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


# FIXME: one notable failure case to handle is if unexpectedly
# running out of staging space
# FIXME: implement retry mechanism
@tasks.cel.task(bind=True, acks_late=True)
def stage_file(self, file_name, target_dir, path_out):
    try:
        end_target = os.path.join(target_dir, file_name)
        tmp_target = os.path.join(target_dir, uuid.uuid4().get_hex())
        logger.info('staging %s' % end_target)
        logger.debug('tmp_target: %s' % tmp_target)
        mars_request = create_mars_request(verb='RETRIEVE',
                                           file_name=file_name,
                                           target=tmp_target)
        logger.debug('mars_request: %s' % mars_request)

        with open(path_out, 'w') as f:
            for rc,fd,l in util.exec_proc([ 'mars' ], logger, stdin=str(mars_request)):
                if fd is not None and l is not None:
                    if fd == 1:
                        logger.debug('fd=%s, l=%s' % (fd, l.strip() if l else l))
                    else:
                        logger.warning('fd=%s, l=%s' % (fd, l.strip() if l else l))
                    f.write(l)
                    f.flush()
            f.flush()
            os.fdatasync(f.fileno())
            f.close()
        if rc != 0:
            logger.error('failed to stage %s, rc = %d' % (file_name, rc))
            logger.error('MARS request was: %s' % mars_request)
            logger.debug('removing temp file %s' % tmp_target)
            util.unlink(tmp_target) # FIXME: use try...finally
            raise TaskFailure('mars returned %d' % rc)

        logger.debug('moving temp file %s -> %s' % (tmp_target, end_target))
        os.rename(tmp_target, end_target)
        logger.info('%s is staged online' % end_target)
        logger.debug('=> invoking scheduler')
        tasks.scheduler.join_staging_task.delay(stage_file.request.id)
    except Exception, e:
        logger.error('stage_file(file_name=%s, target_dir=%s, path_out=%s) '
                     'unexpectedly failed (task id %s): %s, retrying in '
                     '60 s...' %
                     (file_name, target_dir, path_out, stage_file.request.id,
                      str(e)))
        raise self.retry(exc=e, countdown=60)


@tasks.cel.task(bind=True, acks_late=True)
def checksum_file(self, file_path):
    try:
        with open(file_path) as f:
            digest = hashlib.sha1()
            while True:
                bytes = f.read(8 * 1024)
                if bytes == "":
                    break
                digest.update(bytes)
            return digest.hexdigest()
    except Exception, e:
        logger.error('checksum_file(file_path=%s) unexpectedly failed (task id '
                     '%s): %s, retrying in 60 s...' %
                     (file_path, checksum_file.request.id, str(e)))
        raise self.retry(exc=e, countdown=60)


@tasks.cel.task(bind=True, acks_late=True)
def estimate_size(self, file_name):
    try:
        logger.info('calling MARS to estimate size for %s' % file_name)
        size = None
        re_size = re.compile(r'^size=([\d]+);$')
        mars_request = create_mars_request(verb='LIST', file_name=file_name)
        mars_request.params['OUTPUT'] = 'COST'
        logger.debug('mars_request: %s' % mars_request)
        log_fn = logger.debug

        for rc,fd,l in util.exec_proc([ 'mars' ], logger, stdin=str(mars_request)):
            if fd == 2:
                log_fn = logger.warn
            if l is not None:
                log_fn(l.strip())
            if size is None and fd == 1 and l is not None:
                m = re_size.match(l)
                if m:
                    size = int(m.group(1).replace(",", ""))

        assert rc is not None and fd is None and l is None
        # don't trust size if mars returns non-zero
        if rc != 0 or size is None:
            logger.error('failed to compute size, rc = %d' % rc)
            logger.error('MARS request was: %s' % mars_request)
            raise TaskFailure('failed to compute size, rc = %d' % rc)
        elif size == 0:
            logger.error('size is %d' % size)
            logger.error('MARS request was: %s' % mars_request)
            raise HardTaskFailure('size is %d' % size)

        est_size = size * config.FILE_SIZE_WEIGHT + config.FILE_SIZE_EXTRA
        logger.debug('MARS reported size: %d bytes, after compensating: size * '
                     '%d + %d = %d' % \
                     (size, config.FILE_SIZE_WEIGHT, config.FILE_SIZE_EXTRA,
                      est_size))
        logger.info('size is %d' % est_size)
        return est_size
    except HardTaskFailure:
        raise
    except Exception, e:
        logger.error('estimate_size(file_name=%s) unexpectedly failed (task id '
                     '%s): %s, retrying in 60 s...' %
                     (file_name, estimate_size.request.id, str(e)))
        raise self.retry(exc=e, countdown=60)
