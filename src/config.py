import os
from util import util

STAGEDIR = os.path.realpath(os.getenv('STAGEDIR', os.getenv('TMPDIR', '/tmp')))
STAGE_SPACE = int(util.df(STAGEDIR)['1-blocks'])
