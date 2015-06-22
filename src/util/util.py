import errno
import os
import select
import subprocess
import tempfile
from signal import alarm, SIGALRM, signal, siginterrupt

class TimeoutException(Exception): pass

class CommandFailed(Exception): pass

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


def create_tempfile():
    fd, file_name = tempfile.mkstemp()
    os.close(fd)
    return file_name


def unlink(file_name):
    try:
        os.unlink(file_name)
        return True
    except OSError, e:
        if e.errno != errno.ENOENT:
            raise
        return False

def raise_timeout_exception(signum, frame):
    raise TimeoutException


def timed_wait(p, timeout):
    signal(SIGALRM, raise_timeout_exception)
    siginterrupt(SIGALRM, False)
    alarm(timeout)
    rc = p.wait()
    alarm(0)
    return rc


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


def exec_proc(cmd, logger, stdin=None, term_timeout=5):

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
