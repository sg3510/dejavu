from dejavu.database import get_database
import dejavu.decoder as decoder
import fingerprint
import multiprocessing
import os
import traceback
import sys
import re
import json

__PATH__ = "website/app/"
__MONGOLOG_FILE__ = "mongolog.cnf"

# Configure logging
import logging
if os.path.isfile(__MONGOLOG_FILE__):
	with open(__MONGOLOG_FILE__) as f:
	    config = json.load(f)
	from log4mongo.handlers import MongoHandler
	handler = MongoHandler(level = logging.DEBUG ,host=config['host'], capped=config['capped'], port=config['port'], database_name=config['db'], collection=config['collection'], username=config['user'], password=config['passwd'])
else:
	handler = logging.FileHandler("dejavu.log")
	handler.setLevel(logging.DEBUG)
	# create a logging format
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	handler.setFormatter(formatter)

logger = logging.getLogger('Classification_Dejavu')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
logger.info('dejavu initiated')

class Dejavu(object):

	SONG_ID = "song_id"
	SONG_NAME = 'song_name'
	TAG = 'tag'
	CONFIDENCE = 'confidence'
	MATCH_TIME = 'match_time'
	OFFSET = 'offset'
	OFFSET_SECS = 'offset_seconds'

	def __init__(self, config):
		super(Dejavu, self).__init__()

		self.config = config

		# initialize db
		db_cls = get_database(config.get("database_type", None))

		self.db = db_cls(**config.get("database", {}))
		self.db.setup()

		# if we should limit seconds fingerprinted,
		# None|-1 means use entire track
		self.limit = self.config.get("fingerprint_limit", None)
		if self.limit == -1:  # for JSON compatibility
			self.limit = None
		self.get_fingerprinted_songs()

	def get_fingerprinted_songs(self):
		# get songs previously indexed
		# TODO: should probably use a checksum of the file instead of filename
		self.songs = self.db.get_songs()
		self.songnames_set = set()  # to know which ones we've computed before
		for song in self.songs:
			song_name = song[self.db.FIELD_SONGNAME]
			self.songnames_set.add(song_name)

	def erase_bundle(self, user, bundle, admin):
		self.db.delete_bundle(user,bundle,admin)
		logger.info("%s by %s deleted from SQL" % (bundle, user))
		return 0

	def fingerprint_bundle(self, bundle_list, nprocesses=None):
		"Fingerprints a bundle"
		logger.debug('Starting to train bundle.')
		# Try to use the maximum amount of processes if not given.
		try:
			nprocesses = nprocesses or multiprocessing.cpu_count()
		except NotImplementedError:
			nprocesses = 1
		else:
			nprocesses = 1 if nprocesses <= 0 else nprocesses

		pool = multiprocessing.Pool(nprocesses)

		files_to_fingerprint = []
		for file_obj in bundle_list:

			filename = file_obj.file_path+file_obj.file_name

			# don't refingerprint already fingerprinted files
			if decoder.path_to_songname(filename) in self.songnames_set:
				print "%s already fingerprinted, continuing..." % filename
				continue

			files_to_fingerprint.append(file_obj)

		# Prepare _fingerprint_worker input
		worker_input = zip(files_to_fingerprint,
						   [self.limit] * len(files_to_fingerprint))

		# Send off our tasks
		iterator = pool.imap_unordered(_fingerprint_worker,
									   worker_input)

		# Loop till we have all of them
		while True:
			try:
				file_obj, hashes = iterator.next()
			except multiprocessing.TimeoutError:
				continue
			except StopIteration:
				break
			except:
				print("Failed fingerprinting")
				# Print traceback because we can't reraise it here
				traceback.print_exc(file=sys.stdout)
			else:
				log.info("Adding file %s by user %s to SQL" % (file_obj.file_name, file_obj.user))

				sid = self.db.insert_song(file_obj.file_name, file_obj.labeled_as, file_obj.user, file_obj.bundle, file_obj.admin)

				self.db.insert_hashes(sid, hashes, file_obj.labeled_as, file_obj.user, file_obj.bundle, file_obj.admin)
				self.db.set_song_fingerprinted(sid)
				self.get_fingerprinted_songs()

		pool.close()
		pool.join()


	def fingerprint_directory(self, path, extensions, user, bundle, admin, nprocesses=None):
		# Try to use the maximum amount of processes if not given.
		try:
			nprocesses = nprocesses or multiprocessing.cpu_count()
		except NotImplementedError:
			nprocesses = 1
		else:
			nprocesses = 1 if nprocesses <= 0 else nprocesses

		pool = multiprocessing.Pool(nprocesses)

		filenames_to_fingerprint = []
		for filename, _ in decoder.find_files(path, extensions):

			print filename
			# don't refingerprint already fingerprinted files
			if decoder.path_to_songname(filename) in self.songnames_set:
				print "%s already fingerprinted, continuing..." % filename
				continue

			filenames_to_fingerprint.append(filename)

		# Prepare _fingerprint_worker input
		worker_input = zip(filenames_to_fingerprint,
						   [self.limit] * len(filenames_to_fingerprint))

		# Send off our tasks
		iterator = pool.imap_unordered(_fingerprint_worker,
									   worker_input)

		# Loop till we have all of them
		while True:
			try:
				song_name, hashes = iterator.next()
			except multiprocessing.TimeoutError:
				continue
			except StopIteration:
				break
			except:
				print("Failed fingerprinting")
				# Print traceback because we can't reraise it here
				traceback.print_exc(file=sys.stdout)
			else:
				tag = 'Not supplied'

				sid = self.db.insert_song(song_name, tag, user, bundle, admin)

				self.db.insert_hashes(sid, hashes, tag, user, bundle, admin)
				self.db.set_song_fingerprinted(sid)
				self.get_fingerprinted_songs()

		pool.close()
		pool.join()

	def fingerprint_file(self, filepath, tag, song_name=None):
		songname = decoder.path_to_songname(filepath)
		song_name = song_name or songname
		# don't refingerprint already fingerprinted files
		if song_name in self.songnames_set:
			print "%s already fingerprinted, continuing..." % song_name
		else:
			song_name, hashes, tag = _fingerprint_worker(filepath,
													self.limit,
													song_name=song_name)

			sid = self.db.insert_song(song_name, tag)

			self.db.insert_hashes(sid, hashes, tag)
			self.db.set_song_fingerprinted(sid)
			self.get_fingerprinted_songs()

	def find_matches(self, samples, user, bundle, admin, Fs=fingerprint.DEFAULT_FS):
		hashes = fingerprint.fingerprint(samples, Fs=Fs)
		return self.db.return_matches(hashes, user, bundle, admin)

	def align_matches(self, matches):
		"""
			Finds hash matches that align in time with other matches and finds
			consensus about which hashes are "true" signal from the audio.

			Returns a dictionary with match information.
		"""
		# align by diffs
		diff_counter = {}
		largest = 0
		largest_count = 0
		song_id = -1
		for tup in matches:
			sid, diff = tup
			if diff not in diff_counter:
				diff_counter[diff] = {}
			if sid not in diff_counter[diff]:
				diff_counter[diff][sid] = 0
			diff_counter[diff][sid] += 1

			if diff_counter[diff][sid] > largest_count:
				largest = diff
				largest_count = diff_counter[diff][sid]
				song_id = sid

		# extract idenfication
		song = self.db.get_song_by_id(song_id)
		if song:
			# TODO: Clarify what `get_song_by_id` should return.
			songname = song.get(Dejavu.SONG_NAME, None)
			tag = song.get(Dejavu.TAG, None)
		else:
			return None

		# return match info
		nseconds = round(float(largest) / fingerprint.DEFAULT_FS *
						 fingerprint.DEFAULT_WINDOW_SIZE *
						 fingerprint.DEFAULT_OVERLAP_RATIO, 5)
		song = {
			Dejavu.SONG_ID: song_id,
			Dejavu.TAG: tag,
			Dejavu.SONG_NAME: songname,
			Dejavu.CONFIDENCE: largest_count,
			Dejavu.OFFSET: int(largest),
			Dejavu.OFFSET_SECS: nseconds
		}

		return song

	def recognize(self, recognizer, *options, **kwoptions):
		r = recognizer(self)
		return r.recognize(*options, **kwoptions)


def _fingerprint_worker(file, limit=None, song_name=None):
	# Pool.imap sends arguments as tuples so we have to unpack
	# them ourself.

	try:
		filename, limit = file
	except ValueError:
		pass

	print "File Type(%s): %s" %  (filename,type(filename))

	if type(filename) is not str:
		# If file is not a string we assume it is a dict with
		# attributes 'file_path' and 'file_name'
		# not checking for dict directly as type may be ImmutableDict or MongoEngine object
		# and MongoEngine BaseDocument does not inherit dict
		file = filename
		filename = __PATH__+file.file_path + file.file_name

	songname, extension = os.path.splitext(os.path.basename(filename))
	song_name = song_name or songname
	channels, Fs = decoder.read(filename, limit)
	result = set()
	channel_amount = len(channels)

	for channeln, channel in enumerate(channels):
		# TODO: Remove prints or change them into optional logging.
		print("Fingerprinting channel %d/%d for %s" % (channeln + 1,
														channel_amount,
														filename))
		hashes = fingerprint.fingerprint(channel, Fs=Fs)
		print("Finished channel %d/%d for %s" % (channeln + 1, channel_amount,
												 filename))
		result |= set(hashes)

	if type(filename) is not str:
		# return the full file dict if a dict was the input
		return file, result
	else:
		return song_name, result


def chunkify(lst, n):
	"""
	Splits a list into roughly n equal parts.
	http://stackoverflow.com/questions/2130016/splitting-a-list-of-arbitrary-size-into-only-roughly-n-equal-parts
	"""
	return [lst[i::n] for i in xrange(n)]
