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
# djv.fingerprint_bundle(bundles, 3)
# bundles = models.Training_DB.objects(user = 'sebgrubb', bundle = 'dining_room')
djv.erase_bundle('seb','main',False)

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
    cur.execute(djv.db.DELETE_SONG_BUNDLE, (user, bundle, admin))
    # cur.execute(djv.db.DELETE_FINGERPRINT_BUNDLE, (user, bundle, admin))
