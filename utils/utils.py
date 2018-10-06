import boto3
import codecs
import csv
import logging
import numpy as np
import os
import re
import yaml
from utils.exceptions import (
  DownloadException,
  UploadException,
  NoBucketException,
  DeleteException,
  InvalidFileException,
  NoDocumentsException)


logger = logging.getLogger('utils')


def get_vector_average(vectors):
  if len(vectors) == 0:
    raise ValueError("No vectors passed")
  vectors = np.asarray(vectors)
  if len(vectors.shape) != 2:
    raise ValueError("Vectors passed must be of the same dimensionality")
  return np.mean(vectors, axis=0)


def ensure_dir(location, dirname):
  full_path = location + "/" + dirname
  if not os.path.exists(full_path):
    os.makedirs(full_path)


def download_and_read(bucket_name, file_name):
  local_dir = "/tmp/{}".format(bucket_name)
  key_name = "batches/{}".format(file_name)
  download_file(local_dir, key_name, bucket_name=bucket_name)
  try:
    with open("{}/{}".format(local_dir, key_name), encoding='utf-8') as csvDataFile:
      csvReader = csv.reader(csvDataFile)
      return [{'doc_id': str(row[0]), 'text': str(row[1])}
              for row in list(csvReader)[1:]]
  except Exception as e:
    logger.error("{} is an invalid file. Error: {}".format(file_name, e))
    raise InvalidFileException(file_name, e)


def delete_file(key_name, bucket_name=None):
  if bucket_name is None:
    try:
      bucket_name = os.environ["S3_BUCKET_NAME"]
    except Exception as e:
      logger.error(
        "No S3 bucket found. Keyname: {}, Error: {}".format(key_name, e))
      raise NoBucketException(key_name, e)
  client = boto3.client('s3')
  try:
    client.delete_object(Bucket=bucket_name, Key=key_name)
  except Exception as e:
    logger.error("Could not delete {} from {}. Error: {}".format(
      key_name, bucket_name, e))
    raise DeleteException(key_name, bucket_name, e)


def download_file(location, key_name, bucket_name=None):
  ensure_dir(location, os.path.dirname(key_name))
  local_path = "{}/{}".format(location, key_name)
  if not os.path.exists(local_path):
    if bucket_name is None:
      try:
        bucket_name = os.environ["S3_BUCKET_NAME"]
      except Exception as e:
        logger.error(
          "No S3 bucket found. Keyname: {}, Error: {}".format(key_name, e))
        raise NoBucketException(key_name, e)
    client = boto3.client('s3')
    try:
      client.download_file(bucket_name, key_name, local_path)
    except Exception as e:
      logger.error("Could not download {} from {}. Error: {}".format(
        key_name, bucket_name, e))
      raise DownloadException(key_name, bucket_name, e)
    try:
      client._endpoint.http_session.close()
    except Exception as e:
      logger.error("Could not close connection; Error: {}".format(e))


def get_file_contents(file_path, bucket_name):
  try:
    s3 = boto3.resource('s3')
    response = s3.Object(bucket_name, file_path).get()
    return list(csv.DictReader(codecs.getreader('utf-8')(response['Body'])))
  except Exception as e:
    logger.error("Could not download {} from {}. Error: {}".format(
      file_path, bucket_name, e))
    raise DownloadException(file_path, bucket_name, e)


def upload_file(location, keyname, delete_local=True, bucket_name=None):
  ensure_dir(location, os.path.dirname(keyname))
  local_path = "{}/{}".format(location, keyname)
  if bucket_name is None:
    try:
      bucket_name = os.environ["S3_BUCKET_NAME"]
    except Exception as e:
      logger.error(
        "No S3 bucket found. Keyname: {}, Error: {}".format(keyname, e))
      raise NoBucketException(keyname, e)
  s3 = boto3.resource('s3')
  try:
    s3.meta.client.upload_file(local_path, bucket_name, keyname)
  except Exception as e:
    logger.error("Could not upload {} from {} to {}. Error: {}".format(
      keyname, local_path, bucket_name, e))
    raise UploadException(keyname, local_path, bucket_name, e)
  try:
    s3.meta.client._endpoint.http_session.close()
  except Exception as e:
    logger.error("Could not close connection; Error: {}".format(e))


def is_valid_name(name):
  name = str(name).strip()
  if len(name) == 0 or re.match("^[a-zA-Z0-9_.:-]*$", name) is None:
    return False
  return True


def get_stopwords():
  return [
    u'i',
    u'me',
    u'my',
    u'myself',
    u'we',
    u'our',
    u'ours',
    u'ourselves',
    u'you',
    u'your',
    u'yours',
    u'yourself',
    u'yourselves',
    u'he',
    u'him',
    u'his',
    u'himself',
    u'she',
    u'her',
    u'hers',
    u'herself',
    u'it',
    u'its',
    u'itself',
    u'they',
    u'them',
    u'their',
    u'theirs',
    u'themselves',
    u'what',
    u'which',
    u'who',
    u'whom',
    u'this',
    u'that',
    u'these',
    u'those',
    u'am',
    u'is',
    u'are',
    u'was',
    u'were',
    u'be',
    u'been',
    u'being',
    u'have',
    u'has',
    u'had',
    u'having',
    u'do',
    u'does',
    u'did',
    u'doing',
    u'a',
    u'an',
    u'the',
    u'and',
    u'but',
    u'if',
    u'or',
    u'because',
    u'as',
    u'until',
    u'while',
    u'of',
    u'at',
    u'by',
    u'for',
    u'with',
    u'about',
    u'against',
    u'between',
    u'into',
    u'through',
    u'during',
    u'before',
    u'after',
    u'above',
    u'below',
    u'to',
    u'from',
    u'up',
    u'down',
    u'in',
    u'out',
    u'on',
    u'off',
    u'over',
    u'under',
    u'again',
    u'further',
    u'then',
    u'once',
    u'here',
    u'there',
    u'when',
    u'where',
    u'why',
    u'how',
    u'all',
    u'any',
    u'both',
    u'each',
    u'few',
    u'more',
    u'most',
    u'other',
    u'some',
    u'such',
    u'no',
    u'nor',
    u'not',
    u'only',
    u'own',
    u'same',
    u'so',
    u'than',
    u'too',
    u'very',
    u's',
    u't',
    u'can',
    u'will',
    u'just',
    u'don',
    u'should',
    u'now']


def expand_contractions(s):
  ''' Takes a string as input, expands the contracted forms in it and returns the result. '''
  # Source:
  # http://www.englishcoursemalta.com/learn/list-of-contracted-forms-in-english/
  c = {'i\'m': 'i am',
       'you\'re': 'you are',
       'he\'s': 'he is',
       'she\'s': 'she is',
       'we\'re': 'we are',
       'it\'s': 'it is',
       'isn\'t': 'is not',
       'aren\'t': 'are not',
       'they\'re': 'they are',
       'there\'s': 'there is',
       'wasn\'t': 'was not',
       'weren\'t': ' were not',
       'i\'ve': 'i have',
       'you\'ve': 'you have',
       'we\'ve': 'we have',
       'they\'ve': 'they have',
       'hasn\'t': 'has not',
       'haven\'t': 'have not',
       'you\'d': 'you had',
       'he\'d': 'he had',
       'she\'d': 'she had',
       'we\'d': 'we had',
       'they\'d': 'they had',
       'doesn\'t': 'does not',
       'don\'t': 'do not',
       'didn\'t': 'did not',
       'i\'ll': 'i will',
       'you\'ll': 'you will',
       'he\'ll': 'he will',
       'she\'ll': 'she will',
       'we\'ll': 'we will',
       'they\'ll': 'they will',
       'there\'ll': 'there will',
       'i\'d': 'i would',
       'it\'d': 'it would',
       'there\'d': 'there had',
       'there\'d': 'there would',
       'can\'t': 'can not',
       'couldn\'t': 'could not',
       'daren\'t': 'dare not',
       'hadn\'t': 'had not',
       'mightn\'t': 'might not',
       'mustn\'t': 'must not',
       'needn\'t': 'need not',
       'oughtn\'t': 'ought not',
       'shan\'t': 'shall not',
       'shouldn\'t': 'should not',
       'usedn\'t': 'used not',
       'won\'t': 'will not',
       'wouldn\'t': 'would not',
       'what\'s': 'what is',
       'that\'s': 'that is',
       'who\'s': 'who is', }
  # Some forms of 's could either mean 'is' or 'has' but we've made a choice here.
  # Some forms of 'd could either mean 'had' or 'would' but we've made a choice here.
  # Some forms of 'll could wither mean 'will' or 'shall' but we've made a
  # choice here.
  for pat in c:
    s = re.sub(pat, c[pat], s)
  return s


def general_cleaning(s, remove_punct=False, delete_url=True):
  ''' Takes a string as input and False or True for the argument 'remove_punct'.
    Depending on the value of 'remove_punct', all punctuation is either removed, or a space is added before and after.
    Depending on the value or delete_url', URLs are either deleted or all punctuation is replaced by a blank space.
    Removes excessive white space. '''

  # add space to beginning and end of string
  s = ' ' + s + ' '

  if delete_url:
    s = re.sub(r'https?://[^ ]+', ' ', s)
    s = re.sub(r'www\.[^ ]+', ' ', s)
    s = re.sub(r'[^ ]+\.com[^ ]+', ' ', s)

  # Keep currency symbols
  s = re.sub('\$', ' $ ', s)
  s = re.sub('£', ' £ ', s)
  s = re.sub('₤', ' ₤ ', s)
  s = re.sub('€', ' € ', s)
  s = re.sub('¥', ' ¥ ', s)
  s = re.sub('￥', ' ￥ ', s)
  s = re.sub('¢', ' ¢ ', s)

  # remove 's (e.g. girl's -> girl)
  s = re.sub("'s", '', s)

  s = re.sub(r'&amp;', ' and ', s)

  if remove_punct:
    # Remove all sorts of punctuation

    # Handle abbreviations
    p = re.compile(r'( [a-z]\.)([a-z]\.)+')
    l = p.finditer(s)
    contains_abbreviations = False
    for m in l:
      contains_abbreviations = True
      # Get rid of white space in abbreviations
      newbit = re.sub(r' ', '', m.group())
      # change dots in abbreviations into 'PPPP'
      newabbr = re.sub(r'\.', 'PERIOD', newbit)
      s = s.replace(m.group() + ' ', ' ' + newabbr + ' ')  # protect abbreviations

    if contains_abbreviations:
      s = re.sub(r'\.', '', s)  # remove all points that are not in abbreviations
      s = re.sub(r'PERIOD', r'.', s)  # place dots back in abbreviations
    else:
      s = re.sub(r'\.', ' ', s)

    # keep :) :( :0
    s = re.sub(r' :\)(?= )', ' SMILEY', s)
    s = re.sub(r' :\((?= )', ' FROWN', s)
    s = re.sub(r' :0(?= )', ' SURPRISE', s)

    # keep references to decades (or ages)
    s = re.sub(r' 20s ', r' twenties ', s)
    s = re.sub(r' 30s ', r' thirties ', s)
    s = re.sub(r' 40s ', r' fourties ', s)
    s = re.sub(r' 50s ', r' fifties ', s)
    s = re.sub(r' 60s ', r' sixties ', s)
    s = re.sub(r' 70s ', r' seventies ', s)
    s = re.sub(r' 80s ', r' eighties ', s)
    s = re.sub(r' 90s ', r' nineties ', s)

    s = re.sub('[^a-zA-Z.\-\$£₤€¥￥¢]', ' ', s)

    s = re.sub(r" - ", " ", s)
    s = re.sub(r"- ", " ", s)
    s = re.sub(r" -", " ", s)

    s = re.sub('SMILEY', r':)', s)
    s = re.sub('FROWN', r':(', s)
    s = re.sub('SURPRISE', r':0', s)

  else:
    # Add space around all sorts of punctuation.
    # s = re.sub('([^a-zA-Z0-9_-])', r' \1 ', s) # Too agressive!
    s = re.sub(r'\.', ' . ', s)

    # Remove space around abbreviations
    p = re.compile(r'( [a-z] \.)( [a-z] \.)+')
    for m in p.finditer(s):
      newbit = re.sub(' ', '', m.group())
      s = re.sub(m.group() + ' +', ' ' + newbit + ' ', s)

    s = re.sub(r',', ' , ', s)
    s = re.sub(r'\?', ' ? ', s)
    s = re.sub(r'!', ' ! ', s)
    s = re.sub(r' \'([a-z])', r" ' \1", s)
    s = re.sub(r'([a-z])\' ', r"\1 ' ", s)
    s = re.sub(r' \"([a-z])', r' " \1', s)
    s = re.sub(r'([a-z])\" ', r'\1 " ', s)
    s = re.sub(r'\(', ' ( ', s)
    s = re.sub(r'\)', ' ) ', s)
    s = re.sub(r'\[', ' [ ', s)
    s = re.sub(r'\]', ' ] ', s)
    s = re.sub(r'([a-z]): ', r'\1 : ', s)
    s = re.sub(r';', ' ; ', s)
    s = re.sub(r"'s", " 's", s)

  # Remove excessive whitespace
  s = re.sub(r"\s+", " ", s)
  s = re.sub(r"^\s", "", s)
  s = re.sub(r"\s$", "", s)

  return s


def clean_text(s):
  s = s.lower()

  s = re.sub('\n', ' ', s)

  s = re.sub(r"<a href=\"[^\"]+\">([^<]+)</a>", r"\1", s)

  # Put some space between tags and urls or other things. So we don't
  # accidentally remove more than we should a few lines further below this
  # line.
  s = re.sub(r"<", " <", s)
  s = re.sub(r">", "> ", s)

  # remove code blocks
  s = re.sub(r'(<code>).*?(<\/code>)', ' ', s)

  # Remove all tags
  alltags = re.findall(r'(</?)([^>]+)(>)', s)  # list of tuples
  for tag in alltags:
    codetag = tag[0] + tag[1] + tag[2]
    s = re.sub(re.escape(codetag), '', s)

  s = expand_contractions(s)
  s = general_cleaning(s, True, True)

  return s


def get_logger(name):
  FORMAT = '[%(asctime)s] %(name)s: %(levelname)s %(message)s'
  logging.basicConfig(format=FORMAT)
  logger = logging.getLogger(name)

  log_level = os.environ.get("SIMILE_LOG_LEVEL", 'info')
  if log_level == 'debug':
    log_level = logging.DEBUG
  elif log_level == 'error':
    log_level = logging.ERROR
  else:
    log_level = logging.INFO

  logger.setLevel(log_level)
  return logger
