from dejavu import Dejavu
import json
import sys
sys.path.append('../../Server/')
import models
import warnings
warnings.filterwarnings("ignore")

# load config from a JSON file (or anything outputting a python dictionary)
with open("dejavu.cnf") as f:
    config = json.load(f)

# bundles = models.Training_DB.objects(user = 'zack', bundle = 'fa-only3')

# create a Dejavu instance
djv = Dejavu(config)
djv.erase_bundle('sebgrubb','dining_room',False)

bundles = models.Training_DB.objects(user = 'sebgrubb', bundle = 'dining_room')
djv.fingerprint_bundle(bundles, 3)


__PATH__ = "../../Server/website/app/"
fname = '/static/train_db/user/54808cf080d8ab078e26b8f5/main/out_2014-12-17_16-11-53_pc_80e65026a704.wav'
from dejavu.recognize import FileRecognizer
user = 'sebgrubb'
bundle = 'main'
admin = 0
song = djv.recognize(FileRecognizer, __PATH__+fname, user, bundle, admin)
print song


with djv.db.cursor() as cur:
    # cur.execute("DELETE FROM songs WHERE user='seb' AND bundle='main' AND admin=0",)
    # logger.debug(self.DELETE_SONG_BUNDLE % (user, bundle, admin))
    # logger.debug(self.DELETE_FINGERPRINT_BUNDLE % (user, bundle, admin))
    sql_query = djv.db.DELETE_SONG_BUNDLE % (user, bundle, admin)
    cur.execute(sql_query,)
    # cur.execute(djv.db.DELETE_FINGERPRINT_BUNDLE, (user, bundle, admin))




__PATH__ = "../../../Server/website/app/"
fname = '/static/train_db/user/54808cf080d8ab078e26b8f5/main/out_2014-12-17_16-11-53_pc_80e65026a704.wav'
# fname = '/static/train_db/user/54808cf080d8ab078e26b8f5/main/out_2015-02-26_11-59-47.700_web_12131415, 12931220141.wav'
cname = '/static/train_db/eddy/dcase/alert06.wav'

import decoder
# decoder.read(__PATH__+cname)
dobj = decoder.read(__PATH__+fname, 30)[0]

import wavio
# wavio.readwav(__PATH__+cname)[2].T
fs, _, wobj_p = wavio.readwav(__PATH__+fname)
wobj_p = wobj_p.T

wobj = []
for a in wobj_p:
    wobj.append(a)

len(wobj[0])
len(dobj[0])
