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
from tasks.exceptions import TaskFailure
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
@tasks.cel.task(acks_late=True)
def stage_file(file_name, target_dir, path_out):
    logger.info('staging %s' % (os.path.join(target_dir, file_name)))
    try:
        tmp_target = os.path.join(target_dir, uuid.uuid4().get_hex())
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
            logger.debug('removing temp file %s' % tmp_target)
            util.unlink(tmp_target) # FIXME: use try...finally
            raise TaskFailure('mars returned %d' % rc)

        end_target = os.path.join(target_dir, file_name)
        logger.debug('moving temp file %s -> %s' % (tmp_target, end_target))
        os.rename(tmp_target, end_target)
        logger.info('%s is staged online' % end_target)
    finally:
        logger.debug('=> invoking scheduler')
        tasks.scheduler.join_staging_task.delay(stage_file.request.id)


@tasks.cel.task(acks_late=True)
def checksum_file(file_path):
    with open(file_path) as f:
        digest = hashlib.sha1()
        while True:
            bytes = f.read(8 * 1024)
            if bytes == "":
                break
            digest.update(bytes)
        return digest.hexdigest()


@tasks.cel.task(acks_late=True)
def estimate_size(file_name):
    logger.info('calling MARS to estimate size for %s' % file_name)
    size = None
    re_size = re.compile(r'^size=([\d]+);$')
    mars_request = create_mars_request(verb='LIST', file_name=file_name)
    mars_request.params['OUTPUT'] = 'COST'
    logger.debug('mars_request: %s' % mars_request)

    for rc,fd,l in util.exec_proc([ 'mars' ], logger, stdin=str(mars_request)):
        logger.debug('rc = %s, fd = %s, l = %s' % (rc, fd, l.strip() if l else l))
        if size is None and fd == 1:
            m = re_size.match(l)
            if m:
                logger.debug('got match %s' % m.group(1))
                size = int(m.group(1).replace(",", ""))
            else:
                logger.debug('no match')
        elif fd == 2:
            logger.warn(l.strip())

    assert rc is not None and fd is None and l is None
    # don't trust size if mars returns non-zero
    if rc != 0 or size is None:
        logger.error('failed to compute size, rc = %d' % rc)
        raise TaskFailure('failed to compute size, rc = %d' % rc)
    elif size == 0:
        logger.error('size is %d' % size)
        raise TaskFailure('size is %d' % size)

    est_size = size * config.FILE_SIZE_WEIGHT + config.FILE_SIZE_EXTRA
    logger.debug('MARS reported size: %d bytes, after compensating: size * '
                 '%d + %d = %d' % \
                 (size, config.FILE_SIZE_WEIGHT, config.FILE_SIZE_EXTRA,
                  est_size))
    logger.info('size is %d' % est_size)
    return est_size
