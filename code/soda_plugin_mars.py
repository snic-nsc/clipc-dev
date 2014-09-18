# USAGE:
#
# import soda_plugin_mars
# esg_query = { 'VariableName'            : 'tasmin',
#               'Domain'                  : 'EUR-05',
#               'RADrivingName'           : 'SMHI-HIRLAM',
#               'SourceType'              : 'reanalysis',
#               'RADrivingEnsembleMember' : 'r1i1p1',
#               'RAModelName'             : 'SMHI-MESAN',
#               'RAEnsembleMember'        : 'v1',
#               'Frequency'               : 'day',
#               'StartTime'               : '19890101',
#               'EndTime'                 : '19891231' }
# grib_file = 'tasmin_EUR-05_SMHI-HIRLAM_reanalysis_r1i1p1_SMHI-MESAN_v1_day_19890101-19891231.grb'
# soda_plugin_mars.fetch(esg_query, grib_file)

import os
import tempfile
import subprocess
import sys
import zlib

# FIXME: should be configurable with a config file (with e.g path to
# mars binary)


class SodaException(Exception): pass


# FIXME: move out into separate file for unit testing
def _test():

    #tas_EUR-05_SMHI-HIRLAM_reanalysis_r1i1p1_SMHI-MESAN_v1_day_19890101-19891231
    esg_tas = { 'VariableName' : 'tas',
                'Domain' : 'EUR-05',
                'RADrivingName' : 'SMHI-HIRLAM',
                'SourceType' : 'reanalysis',
                'RADrivingEnsembleMember' : 'r1i1p1',
                'RAModelName' : 'SMHI-MESAN',
                'RAEnsembleMember' : 'v1',
                'Frequency' : 'day',
                'StartTime' : '19890101',
                'EndTime' : '19891231' }

    mars_tas = '''LIST,
   DATABASE = marsre,
   DATE     = 19890101/TO/19891231/BY/1,
   LEVTYPE  = HL,
   LEVELIST = 2,
   TIME     = 0000/0300/0600/0900/1200/1500/1800/2100,
   STEP     = 0,
   PARAM    = 11.1,
   CLASS    = re,
   TYPE     = fc,
   STREAM   = REAN,
   EXPVER   = e4dd,
   MODEL    = MESAN
'''

    #pr_EUR-05_SMHI-HIRLAM_reanalysis_r1i1p1_SMHI-MESAN_v1_day_19890101-19891231
    esg_pr = { 'VariableName' : 'pr',
               'Domain' : 'EUR-05',
               'RADrivingName' : 'SMHI-HIRLAM',
               'SourceType' : 'reanalysis',
               'RADrivingEnsembleMember' : 'r1i1p1',
               'RAModelName' : 'SMHI-MESAN',
               'RAEnsembleMember' : 'v1',
               'Frequency' : 'day',
               'StartTime' : '19890101',
               'EndTime' :  '19891231' }

    mars_pr = '''LIST,
   DATABASE = marsre,
   DATE     = 19890101/TO/19891231/BY/1,
   LEVTYPE  = HL,
   LEVELIST = 0,
   TIME     = 0000,
   STEP     = 0,
   PARAM    = 164.129,
   CLASS    = re,
   TYPE     = fc,
   STREAM   = REAN,
   EXPVER   = e4dd,
   MODEL    = MESAN
'''

    #tasmax_EUR-05_SMHI-HIRLAM_reanalysis_r1i1p1_SMHI-MESAN_v1_day_19890101-19891231
    esg_tasmax = { 'VariableName' : 'tasmax',
                   'Domain' : 'EUR-05',
                   'RADrivingName' : 'SMHI-HIRLAM',
                   'SourceType' : 'reanalysis',
                   'RADrivingEnsembleMember' : 'r1i1p1',
                   'RAModelName' : 'SMHI-MESAN',
                   'RAEnsembleMember' : 'v1',
                   'Frequency' : 'day',
                   'StartTime' : '19890101',
                   'EndTime' :  '19891231' }

    mars_tasmax = '''LIST,
   DATABASE = marsre,
   DATE     = 19890101/TO/19891231/BY/1,
   LEVTYPE  = HL,
   LEVELIST = 2,
   TIME     = 1800,
   STEP     = 0,
   PARAM    = 15.1,
   CLASS    = re,
   TYPE     = fc,
   STREAM   = REAN,
   EXPVER   = e4dd,
   MODEL    = MESAN
'''

    #tasmin_EUR-05_SMHI-HIRLAM_reanalysis_r1i1p1_SMHI-MESAN_v1_day_19890101-19891231
    esg_tasmin = { 'VariableName' : 'tasmin',
                   'Domain' : 'EUR-05',
                   'RADrivingName' : 'SMHI-HIRLAM',
                   'SourceType' : 'reanalysis',
                   'RADrivingEnsembleMember' : 'r1i1p1',
                   'RAModelName' : 'SMHI-MESAN',
                   'RAEnsembleMember' : 'v1',
                   'Frequency' : 'day',
                   'StartTime' : '19890101',
                   'EndTime' :  '19891231' }

    mars_tasmin = '''LIST,
   DATABASE = marsre,
   DATE     = 19890101/TO/19891231/BY/1,
   LEVTYPE  = HL,
   LEVELIST = 2,
   TIME     = 0600,
   STEP     = 0,
   PARAM    = 16.1,
   CLASS    = re,
   TYPE     = fc,
   STREAM   = REAN,
   EXPVER   = e4dd,
   MODEL    = MESAN
'''

    assert MarsQueryFrom(esg_tas).request('LIST').strip() == mars_tas.strip()
    assert MarsQueryFrom(esg_pr).request('LIST').strip() == mars_pr.strip()
    assert MarsQueryFrom(esg_tasmin).request('LIST').strip() == mars_tasmin.strip()
    assert MarsQueryFrom(esg_tasmax).request('LIST').strip() == mars_tasmax.strip()

    print 'id of %s = %s' % (esg_tas, offline_content_id(esg_tas))
    fetch(esg_tas, 'tas.grb')

    print 'id of %s = %s' % (esg_pr, offline_content_id(esg_pr))
    fetch(esg_pr, 'pr.grb')

    print 'id of %s = %s' % (esg_tasmax, offline_content_id(esg_tasmax))
    fetch(esg_tasmax, 'tasmax.grb')

    print 'id of %s = %s' % (esg_tasmin, offline_content_id(esg_tasmin))
    fetch(esg_tasmin, 'tasmin.grb')


#VariableName_Domain_RADrivingName_SourceType_RADrivingEnsembleMember_RAModelName_RAEnsembleMember_Frequency_StartTime-EndTime.nc
#tas         _EUR-05_SMHI-HIRLAM_  reanalysis_r1i1p1                 _SMHI-MESAN_ v1_              day_      19890101- 19891231.nc
#RETRIEVE,
#    DATABASE   = marsre,
#    DATE       = 19890101/TO/19890131/BY/1,
#    LEVTYPE    = HL,
#    LEVELIST   = 2,
#    TIME       = 0000/0300/0600/0900/1200/1500/1800/2100,
#    STEP       = 0,
#    PARAM      = 11.1,
#    CLASS      = RE,
#    TYPE       = FC,
#    STREAM     = REAN,
#    EXPVER     = e4dd,
#    MODEL      = MESAN,
#
# just for testing
def _is_a_tas_EUR_05_SMHI_HIRLAM_reanalysis_r1i1p1_SMHI_MESAN_v1_day(esgf_query):
    return esgf_query.get('VariableName', '')            == 'tas'          and \
           esgf_query.get('Domain', '')                  == 'EUR-05'       and \
           esgf_query.get('RADrivingName', '')           == 'SMHI-HIRLAM'  and \
           esgf_query.get('SourceType', '')              == 'reanalysis'   and \
           esgf_query.get('RADrivingEnsembleMember', '') == 'r1i1p1'       and \
           esgf_query.get('RAModelName', '')             == 'SMHI-MESAN'   and \
           esgf_query.get('RAEnsembleMember', '')        == 'v1'           and \
           esgf_query.get('Frequency', '')               == 'day'


#VariableName_Domain_RADrivingName_SourceType_RADrivingEnsembleMember_RAModelName_RAEnsembleMember_Frequency_StartTime-EndTime.nc
#pr_EUR-05_SMHI-HIRLAM_reanalysis_r1i1p1_SMHI-MESAN_v1_day_19890101-19891231.nc
#RETRIEVE,
#    DATABASE   = marsre,
#    DATE       = 19890101/TO/19890131/BY/1,
#    LEVTYPE    = HL,
#    LEVELIST   = 0,
#    TIME       = 0000,
#    STEP       = 0,
#    PARAM      = 164.129,
#    CLASS      = RE,
#    TYPE       = FC,
#    STREAM     = REAN,
#    EXPVER     = e4dd,
#    MODEL      = MESAN,
# just for testing
def _is_a_pr_EUR_05_SMHI_HIRLAM_reanalysis_r1i1p1_SMHI_MESAN_v1_day(esgf_query):
    return esgf_query.get('VariableName', '')            == 'pr'           and \
           esgf_query.get('Domain', '')                  == 'EUR-05'       and \
           esgf_query.get('RADrivingName', '')           == 'SMHI-HIRLAM'  and \
           esgf_query.get('SourceType', '')              == 'reanalysis'   and \
           esgf_query.get('RADrivingEnsembleMember', '') == 'r1i1p1'       and \
           esgf_query.get('RAModelName', '')             == 'SMHI-MESAN'   and \
           esgf_query.get('RAEnsembleMember', '')        == 'v1'           and \
           esgf_query.get('Frequency', '')               == 'day'


#VariableName_Domain_RADrivingName_SourceType_RADrivingEnsembleMember_RAModelName_RAEnsembleMember_Frequency_StartTime-EndTime.nc
#tasmax_EUR-05_SMHI-HIRLAM_reanalysis_r1i1p1_SMHI-MESAN_v1_day_19890101-19891231.nc
#RETRIEVE,
#    DATABASE   = marsre,
#    DATE       = 19890101/TO/19890131/BY/1,
#    LEVTYPE    = HL,
#    LEVELIST   = 2,
#    TIME       = 1800,
#    STEP       = 0,
#    PARAM      = 15.1,
#    CLASS      = RE,
#    TYPE       = FC,
#    STREAM     = REAN,
#    EXPVER     = e4dd,
#    MODEL      = MESAN,
# just for testing
def _is_a_tasmax_EUR_05_SMHI_HIRLAM_reanalysis_r1i1p1_SMHI_MESAN_v1_day(esgf_query):
    return esgf_query.get('VariableName', '')            == 'tasmax'       and \
           esgf_query.get('Domain', '')                  == 'EUR-05'       and \
           esgf_query.get('RADrivingName', '')           == 'SMHI-HIRLAM'  and \
           esgf_query.get('SourceType', '')              == 'reanalysis'   and \
           esgf_query.get('RADrivingEnsembleMember', '') == 'r1i1p1'       and \
           esgf_query.get('RAModelName', '')             == 'SMHI-MESAN'   and \
           esgf_query.get('RAEnsembleMember', '')        == 'v1'           and \
           esgf_query.get('Frequency', '')               == 'day'


#VariableName_Domain_RADrivingName_SourceType_RADrivingEnsembleMember_RAModelName_RAEnsembleMember_Frequency_StartTime-EndTime.nc
#tasmin_EUR-05_SMHI-HIRLAM_reanalysis_r1i1p1_SMHI-MESAN_v1_day_19890101-19891231.nc
#RETRIEVE,
#    DATABASE   = marsre,
#    DATE       = 19890101/TO/19890131/BY/1,
#    LEVTYPE    = HL,
#    LEVELIST   = 2,
#    TIME       = 0600,
#    STEP       = 0,
#    PARAM      = 16.1,
#    CLASS      = RE,
#    TYPE       = FC,
#    STREAM     = REAN,
#    EXPVER     = e4dd,
#    MODEL      = MESAN,
# just for testing
def _is_a_tasmin_EUR_05_SMHI_HIRLAM_reanalysis_r1i1p1_SMHI_MESAN_v1_day_19890101(esgf_query):
    return esgf_query.get('VariableName', '')            == 'tasmin'       and \
           esgf_query.get('Domain', '')                  == 'EUR-05'       and \
           esgf_query.get('RADrivingName', '')           == 'SMHI-HIRLAM'  and \
           esgf_query.get('SourceType', '')              == 'reanalysis'   and \
           esgf_query.get('RADrivingEnsembleMember', '') == 'r1i1p1'       and \
           esgf_query.get('RAModelName', '')             == 'SMHI-MESAN'   and \
           esgf_query.get('RAEnsembleMember', '')        == 'v1'           and \
           esgf_query.get('Frequency', '')               == 'day'


# TODO: this should really be configurable, in a separate file for
# example - we don't want to modify this code directly if we need to
# update the mapping...
#
# FIXME: implement a separate general and flexible MARS request
# builder
class MarsQueryFrom(object):

    def __init__(self, esgf_query):
        assert type(esgf_query) is dict
        assert all(type(x) is str for x in esgf_query.values())
        assert 'VariableName' in esgf_query
        assert 'Domain' in esgf_query
        assert 'RADrivingName' in esgf_query
        assert 'SourceType' in esgf_query
        assert 'RADrivingEnsembleMember' in esgf_query
        assert 'RAModelName' in esgf_query
        assert 'RAEnsembleMember' in esgf_query
        assert 'Frequency' in esgf_query
        assert 'StartTime' in esgf_query
        assert 'EndTime' in esgf_query

        self.esgf_query = esgf_query

    def request(self, verb, target=None):
        assert type(verb) is str
        assert target is None or type(target) is str

        return verb.upper()                     + ',\n' + \
            '   DATABASE = ' + self._database() + ',\n' + \
            '   DATE     = ' + self._date()     + ',\n' + \
            '   LEVTYPE  = ' + self._levtype()  + ',\n' + \
            '   LEVELIST = ' + self._levelist() + ',\n' + \
            '   TIME     = ' + self._time()     + ',\n' + \
            '   STEP     = ' + self._step()     + ',\n' + \
            '   PARAM    = ' + self._param()    + ',\n' + \
            '   CLASS    = ' + self._class()    + ',\n' + \
            '   TYPE     = ' + self._type()     + ',\n' + \
            '   STREAM   = ' + self._stream()   + ',\n' + \
            '   EXPVER   = ' + self._expver()   + ',\n' + \
            '   MODEL    = ' + self._model()    + \
            ( ',\n   TARGET   = ' + "'" + target + "'" if target else '' ) + '\n'

    def _database(self):
        return 'marsre'

    def _date(self):
        assert self.esgf_query['Frequency'] == 'day'
        start = self.esgf_query['StartTime']
        end = self.esgf_query['EndTime']
        return '%s/TO/%s/BY/1' % ( start, end )

    def _levtype(self):
        return 'HL'

    def _levelist(self):
        if self.esgf_query['VariableName'] == 'tas':
            return '2'
        elif self.esgf_query['VariableName'] == 'pr':
            return '0'
        elif self.esgf_query['VariableName'] == 'tasmax':
            return '2'
        elif self.esgf_query['VariableName'] == 'tasmin':
            return '2'
        raise AssertionError

    def _time(self):
        if self.esgf_query['VariableName'] == 'tas':
            return '0000/0300/0600/0900/1200/1500/1800/2100'
        elif self.esgf_query['VariableName'] == 'pr':
            return '0000'
        elif self.esgf_query['VariableName'] == 'tasmax':
            return '1800'
        elif self.esgf_query['VariableName'] == 'tasmin':
            return '0600'
        raise AssertionError

    def _step(self):
        return '0'

    def _param(self):
        if self.esgf_query['VariableName'] == 'tas':
            return '11.1'
        elif self.esgf_query['VariableName'] == 'pr':
            return '164.129'
        elif self.esgf_query['VariableName'] == 'tasmax':
            return '15.1'
        elif self.esgf_query['VariableName'] == 'tasmin':
            return '16.1'
        raise AssertionError

    def _class(self):
        return 're'

    def _type(self):
        return 'fc'

    def _stream(self):
        return 'REAN'

    def _expver(self):
        return 'e4dd'

    def _model(self):
        return 'MESAN'


# raises SodaException
def _mars_client(request, stdout, stderr):
    assert type(request) is str
    assert stdout is None or type(stdout) is int or type(stdout) is file
    assert stderr is None or type(stderr) is int or type(stderr) is file

    # FIXME: make hardcoded value of 'mars' configurable
    args = [ 'mars' ]
    p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=stdout,
                         stderr=stderr, shell=False)
    # provide the request to the mars client on stdin
    out,err = p.communicate(request)

    # is only anything but None if we have stdout=subprocess.PIPE or
    # stderr=subprocess.PIPE, but that input is not allowed
    assert out is None and err is None

    ret = p.wait()

    # is only None if we're still waiting for it to complete, but we
    # should exec synchronously
    assert ret is not None

    if ret < 0:
        raise SodaException("%s failed, ret=%d" % args, ret);


def offline_content_id(esgf_request, stderr=None):
    """Compute an identifier of the expected offline content for a
    given esgf_request without actually reading all that content, such
    that whenever offline content is changed, its offline_content_id
    should also change.

    This will only promise to return OK if a previous call to
    is_providing for the same value of esgf_request has returned True,
    otherwise it may very well raise a SodaException.

    It is allowed for the id to change without the content to change,
    but if the offline content has changed the id should also change

    This may be used by the caller to perform cache invalidation. It
    should not be relied upon to determine whether different requests
    will return the same data. For example using mtime and file size
    as basis for the offline_content_id is OK, but there is obviously
    a very high chance there are several other files having the same
    id.

    Args:
        esgf_request: a type dict(str, str) containing ESGF keywords.
        stderr: type int or type file, an optional file descriptor or
            file where error will be sent during the fetch (used as
            stderr in subprocess.Popen).
    Returns:
        A string representing the id.
    Raises:
        SodaException: A critical error occurred.
    """
    assert type(esgf_request) is dict
    assert stderr is None or type(stderr) is int or type(stderr) is file

    # FIXME: generate a MARS list request with most output removed,
    #        except file[.] of course
    mars_request = MarsQueryFrom(esgf_request).request('LIST')
    args = [ 'mars' ]
    p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=stderr, shell=False)

    # FIXME: this might lock forever if mars_request is large enough
    # (4k?), should use non-blocking I/O
    p.stdin.write(mars_request)
    p.stdin.close()

    # For MARS we compute a crc from all the lines prefixed with
    # file[. Since the referred files may very well contain fields not
    # actually in this request, the file(s) might be modified without
    # really changing the content for this request. But that is OK
    # anyway - it will only be one more unnecessary cache
    # invalidation.
    #
    # we're compute a crc over all the lines in this format: file[0] =
    # TSMmarsre:/tsm:/marsre/rean/e4dd/mesan/fc/19890101/hl/87844:/20140904.001957
    # this output is expected to change if data is re-archived. It
    # will also be changed if other fields not in this request but in
    # the same file are re-archived.

    crc_sum = 0
    for line in iter(p.stdout.readline, ''):
        # it would be more robust to use a regex (but also use more
        # CPU). It's reasonable to assume anything starting with file[
        # really is describing the actual MARS file handle on the
        # server side (FIXME: verify this assumption):
        if line.startswith('file['):
            # sys.stdout.write('computing crc of: ' + line)
            crc_sum = zlib.crc32(line, crc_sum)

    ret = p.wait()
    assert ret is not None
    if ret < 0:
        raise SodaException("%s failed, ret=%d" % args, ret);

    return str(crc_sum)


def fetch(esgf_request, result_path, stdout=None, stderr=None):
    """Retrieve the offline data into result_path for a ESGF query.

    This method will fetch new data every time it is called. The
    existence of result_path can be used by the caller to determine if
    the file is already prefetched or not. offline_content_id should
    be used by the caller to determine if the content of the existing
    file is stale or not.

    While data is being fetched, content is written into temporary
    storage. Once finished OK, an atomic move of the temporary file to
    result_path is performed.

    Note: uses dirname of result_path as temporary storage for the
    resulting files. This may be changed by setting env variables
    TMPDIR, TEMP or TMP (not recommended).

    Args:
        esgf_request: a type dict(str, str) containing ESGF keywords.
        result_path: a type str where the resulting data will be
            retrieved into.
        stdout: type int or type file, an optional file descriptor or
            file where output will be sent during the fetch (used as
            stdout in subprocess.Popen)
        stderr: type int or type file, an optional file descriptor or
            file where error will be sent during the fetch (used as
            stderr in subprocess.Popen).
    Returns:
        None
    Raises:
        SodaException: A critical error occurred.
    """
    assert type(esgf_request) is dict
    assert result_path is None or type(result_path) is str
    assert stdout is None or type(stdout) is int or type(stdout) is file
    assert stderr is None or type(stderr) is int or type(stderr) is file

    # FIXME: handle signals
    dirname = os.path.dirname(result_path)
    fd,tempName = tempfile.mkstemp(prefix='.soda_tmp', dir=dirname)

    try:
        mars_request = MarsQueryFrom(esgf_request).request('RETRIEVE', tempName)
        _mars_client(mars_request, stdout, stderr)
        # FIXME: validate the sync will correctly flush data to disk,
        # even if we haven't used the fd to write.
        os.fsync(fd)
        os.close(fd)
        os.rename(tempName, result_path)
    except (AssertionError, SodaException):
        raise
    except BaseException as e:
        # it would be nice to properly chain exceptions (python3
        # supports this)
        raise SodaException('request of %s -> %s failed: %s' % (esgf_request, result_path, str(e)))
    finally:
        if os.path.exists(tempName):
            os.unlink(tempName)


def is_providing(esgf_request):
    """Return if the given esgf_request is handled by this plugin or
    not.

    If this returns True, a later call to fetch or
    offline_content_id is expected to execute OK.

    If this returns False, a later call to fetch or
    offline_content_id is expected to raise a SodaException.

    The implementation is not required to actually query the offline
    storage system, it just needs to be able to map the esgf_request
    to a proper query that is expected to return offline content.

    Args:
        esgf_request: a type dict(str, str) containing ESGF keywords.
    Returns:
        a type bool indicating if esgf_query is handled by this plugin
        or not.
    """
    assert type(esgf_request) is dict

    return _is_a_tasmin_EUR_05_SMHI_HIRLAM_reanalysis_r1i1p1_SMHI_MESAN_v1_day_19890101(esgf_request) or \
           _is_a_tasmax_EUR_05_SMHI_HIRLAM_reanalysis_r1i1p1_SMHI_MESAN_v1_day(esgf_request)          or \
           _is_a_pr_EUR_05_SMHI_HIRLAM_reanalysis_r1i1p1_SMHI_MESAN_v1_day(esgf_request)              or \
           _is_a_tas_EUR_05_SMHI_HIRLAM_reanalysis_r1i1p1_SMHI_MESAN_v1_day(esgf_request)
