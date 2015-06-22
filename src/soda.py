from __future__ import absolute_import
import config
import hashlib
import logging
import os.path
import simplejson as json
import tasks
import tasks.scheduler as scheduler
import uuid
from collections import OrderedDict
from flask import Flask, jsonify, redirect, request, send_from_directory, \
    url_for
from models import db, DownloadRequest, StagableFile
from sqlalchemy.sql import func


app = Flask('soda')
app.config.from_object(config)
db.init_app(app)


def getEsgfQuery(request_args):

	where=os.path.dirname(__file__)
	facetfile=os.path.join(where,"esgf-mars-facet-mapping")
	mappingsfile=os.path.join(where,"esgf-mars-default-mapping")
	attribsfile=os.path.join(where,"esgf-mars-attrib-mapping")
	mappingsdict=OrderedDict()
	attribsdict=OrderedDict()
	facetsdict=OrderedDict()
	fp=open(mappingsfile,'r')
	mappingsdict=json.load(fp,object_pairs_hook=OrderedDict)
	fp.close()
	fp=open(attribsfile,'r')
	attribsdict=json.load(fp,object_pairs_hook=OrderedDict)
	fp.close()
	fp=open(facetfile,'r')
	facetsdict=json.load(fp,object_pairs_hook=OrderedDict)
	fp.close()
        variablename=''
        freqval=''
	try:
		for facet,backend in facetsdict.iteritems():
			val=request_args.get(facet)
			if val!= None:
				for backendopt in backend:
					if attribsdict.__contains__(backendopt):
						if facet =='variable':
							variablename=val
						if facet =='frequency':
							freqval=val

						#we have a match for the facet..
						if attribsdict[backendopt].__contains__(val):
							#we even have a substitution value for the specified facet value
							#print 'backend value for user supplied facet value %s is %s'%(val,attribsdict[backendopt][val])
							sub=attribsdict[backendopt][val]
							#print 'before change: mappingsdict val=%s'%(mappingsdict[backendopt])
							mappingsdict[backendopt]=sub
							#print 'after change: mappingsdict val=%s'%(mappingsdict[backendopt])
				#print "%s:%s"%(facet,val)
		mappingsdict['date']=mappingsdict['datestr']
		mappingsdict['date']+=str(mappingsdict['freq'])
		mappingsdict.pop('datestr')
		mappingsdict.pop('freq')
	except:
		raise
	fn='%s_Eur05_SMHI-HIRLAM_RegRean_v0d0_SMHI-MESAN_v1_%s.grb'%(variablename,freqval)
	return (fn,mappingsdict)


class APIException(Exception):
    def __init__(self, rc, message, payload=None):
        Exception.__init__(self)
        self.rc = rc
        self.message = message
        self.payload = payload if payload is not None else {}

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return '%s(rc=%s, message=%s, payload=%s' % \
            ( type(self).__name__, self.rc, self.message, self.payload )

class HTTPBadRequest(APIException):
    def __init__(self, message, payload=None):
        APIException.__init__(self, 400, message, payload)

class HTTPUnauthorized(APIException):
    def __init__(self, message, payload=None):
        APIException.__init__(self, 401, message, payload)

class HTTPForbidden(APIException):
    def __init__(self, message, payload=None):
        APIException.__init__(self, 403, message, payload)

class HTTPNotfound(APIException):
    def __init__(self, message, payload=None):
        APIException.__init__(self, 404, message, payload)

class HTTPInternalError(APIException):
    def __init__(self, message, payload=None):
        APIException.__init__(self, 500, message, payload)

@app.errorhandler(APIException)
def handle_api_exception(error):
    logger.debug(error)
    return ( jsonify(message=error.message, payload=error.payload),
             error.rc )

#@app.errorhandler(AssertionError)
#def handle_api_assertion_error(error):
#    raise APIException(500, str(error))

# Note: this won't be called if flask is running in debug mode
@app.errorhandler(500)
def handle_api_internal_error(error):
    logger.error(str(error))
    return ( jsonify(message='Internal Error', payload=str(error)), 500 )

@app.route('/request', methods=['POST'])
def create_request():
    logger.debug(request)
    r = select_request_input(request)
    openid = 'https://esg-dn1.nsc.liu.se/esgf-idp/openid/perl'
#    openid = r.get('openid')
    if not openid:
        logger.warn('faulty request %s is missing attribute openid' % request)
        raise HTTPBadRequest('missing attribute openid (string)')
    assert type(openid) is str or type(openid) is unicode, openid

    name,params = getEsgfQuery(r)
    if not params:
        logger.warn('failed to provide MARS params from request: %s' % request)
        raise HTTPBadRequest('failed to provide MARS params from '
                             'request: %s' % request, payload=str(request))

    file_to_query = { name : ',\n'.join('%s = %s' % (k,v) for k,v in params.iteritems()) }
    r_uuid = uuid.uuid4().get_hex()
    logger.info('=> registering new request: uuid=%s openid=%s, '
                'file_to_query=%s' % (r_uuid, openid, file_to_query))
    scheduler.register_request.apply_async((r_uuid, openid, file_to_query),
                                           task_id=r_uuid)
    return jsonify(), 201, { 'location': '/request/%s' % r_uuid }


def select_request_input(request):
    content_type = request.headers['Content-Type']
    selector = { 'application/json' : request.json,
                 'application/x-www-form-urlencoded' : request.args }
    r = selector.get(content_type)
    if not r:
        raise HTTPBadRequest('Unsupported HTTP Content-Type %s - must be any '
                             'of %s' % \
                             (content_type, ', '.join(selector.keys())))
    return r

@app.route('/mars', methods=['POST'])
def mars():
    logger.debug(request)
    r = select_request_input(request)     # where r is a dict()
    # FIXME: sanitize this, don't let choose verb keywords, we also
    # _want_ to set target ourselves for each sub request
    openid = r.get('openid')
    if not openid:
        logger.warn('faulty request %s is missing attribute openid' % request)
        raise HTTPBadRequest('missing attribute openid (string)')
    assert type(openid) is str or type(openid) is unicode, openid
    verb = r.get('verb').upper()
    if not verb:
        raise HTTPBadRequest('missing MARS request verb (string)')
    if verb != 'RETRIEVE':
        raise HTTPBadRequest('only RETRIEVE is supported for the moment')
    # NOTE: v.upper() is safe for all except strings e.g. not TARGET,
    # but that one we ignore anyway:
    params = dict((k.upper(),v.upper()) for k,v in r.get('params').iteritems())
    if not params:
        raise HTTPBadRequest('missing params (dict of string) - any valid '
                             'MARS request keyword')
    if 'TARGET' in params:
        del params['TARGET']
    logger.debug('request verb: %s' % verb)
    logger.debug('request params: %s' % params)
    # NOTE: there are many params that map to the same canonical MARS
    # request, but we cannot detect that
    mars_request_text = ',\n'.join('%s = %s' % (k,v) for k,v in sorted(params.iteritems()))
    file_name = 'mars_%s.grb' % hashlib.sha1(mars_request_text).hexdigest()
    logger.debug('file name: %s' % file_name)

    file_to_query = { file_name : mars_request_text }
    r_uuid = uuid.uuid4().get_hex()
    logger.debug('=> registering new request: uuid=%s openid=%s, '
                 'file_to_query=%s' % (r_uuid, openid, file_to_query))
    scheduler.register_request.apply_async((r_uuid, openid, file_to_query),
                                           task_id=r_uuid)
    return jsonify(), 201, { 'location': '/request/%s' % r_uuid }


# this would allow for the wget script to signal that all files have
# been downloaded, enabling space to be freed up as early as possible
@app.route('/request', methods=['DELETE'])
def delete_request():
    pass # TODO: only allow esg node or the request owner to delete a request


# FIXME: only to be called by matching openid
@app.route('/request/<uuid>', methods=['GET'])
def status_request(uuid):
    #assert request.headers['Content-Type'] == 'application/json', request
    #assert 'openid' in request.json, request.json
    logger.debug('received request uuid %s' % uuid)
    r = DownloadRequest.query.get(uuid)
    if not r:
        future = tasks.cel.AsyncResult(uuid)
        return jsonify(status=future.status.lower(),
                       staged_files=[],
                       offline_files=[]), 200, { 'location' : '/request/%s' % uuid }

    all_files = set(r.files)
    staged = set(f for f in all_files if f.state == 'online')
    offline = all_files - staged
    is_done = len(offline) == 0
    urls_offline_files = [ url_for('status_file', uuid=r.uuid, file_name=x.name, _external=True) for x in offline ]
    urls_staged_files = [ url_for('serve_file', uuid=r.uuid, file_name=x.name, _external=True) for x in staged ]
    return jsonify(status=r.state,
                   staged_files=urls_staged_files,
                   offline_files=urls_offline_files), 200, { 'location' : '/request/%s' % r.uuid }


def get_file_from_request(uuid, file_name):
    sf = StagableFile.query.\
        join(StagableFile.requests).\
        filter(StagableFile.name == file_name).\
        filter(DownloadRequest.uuid == uuid).first()
    if not sf:
        raise HTTPNotfound('found no registered request with uuid %s '
                           'containing file %s' % (uuid, file_name))
    return sf

@app.route('/request/<uuid>/status/<file_name>', methods=['GET'])
def status_file(uuid, file_name):
    sf = get_file_from_request(uuid, file_name)
    if sf.staging_task:
        return jsonify(output=sf.staging_task.output()), 200
    else:
        return jsonify(output='no task status information available for %s' % file_name), 200


# set time of cache expiry?
# use some reference counting scheme? but we cannot reliably detect
# successful downloads?

# possibly use http content disposition?
@app.route('/request/<uuid>/staged/<file_name>', methods=['GET'])
def serve_file(uuid, file_name):
    sf = get_file_from_request(uuid, file_name)
    sf.time_accessed = func.now()
    db.session.commit()
    return redirect(url_for('render_static', uuid=uuid, file_name=file_name))


# FIXME: REMOVEME: should be served from apache or whatever will be
#        running in production
# FIXME: implement authorization - you're not allowed to download any
#        files just like that even if they happen to be available.
# X-Sendfile
# http://www.yiiframework.com/wiki/129/x-sendfile-serve-large-static-files-efficiently-from-web-applications/
# http://pythonhosted.org/xsendfile/
@app.route('/static/<uuid>/staged/<file_name>', methods=['GET'])
def render_static(uuid, file_name):
    sf = get_file_from_request(uuid, file_name)
    return send_from_directory(sf.path, sf.name)


if __name__ == '__main__':
    logger = logging.getLogger('soda')
    ch = logging.StreamHandler()
    #ch.setFormatter(logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s', datefmt='%Y-%m-%dT%H:%M:%S'))
    ch.setFormatter(logging.Formatter(fmt=app.config['LOGFORMAT']))
    logger.addHandler(ch)
    logger.setLevel(logging.INFO)
    app.run(debug=True, host='0.0.0.0')
