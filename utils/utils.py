import re

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
