import pandas as pd
import utils.utils as utils
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.stem import WordNetLemmatizer
from textblob import TextBlob
from skopt import gp_minimize
from skopt.space import Real
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import cross_val_score

df = pd.read_csv('/Users/sunny.chopra/Downloads/AP_ICD10.tsv', sep='\t')
# ['chf cmplnt', 'A/P', 'icd10encounterdiagcode', 'icd10encounterdiagdescr']

df['target'] = df['icd10encounterdiagcode'].str.slice(0,3)

df_randomized = df.sample(frac=1.0, random_state=0)

# Dropping N/A's
df_randomized = df_randomized.dropna()

n_features = 1000
tf_vectorizer = TfidfVectorizer(
  stop_words='english',
  max_df=0.98,
  min_df=3,
  max_features=n_features)
cleaned_texts = []
for t in list(df_randomized.loc[:, 'A/P']):
  # Doing some cleaning of the text
  t = str(t)
  blob = TextBlob(t)
  good_words = [n for n, t in blob.tags if t == 'NN' or t == 'JJ']
  good_words = utils.clean_text(' '.join(good_words))
  wnl = WordNetLemmatizer()
  good_words = [wnl.lemmatize(i) for i in good_words.split(' ')]
  cleaned_texts.append(' '.join(good_words))
tf = tf_vectorizer.fit_transform(cleaned_texts)

# Bayesian optimization
def optimize(x):
  clf = SGDClassifier(loss=x[0], penalty=x[1], alpha=x[2], random_state=0)
  scores = cross_val_score(clf, tf.toarray(), df_randomized['target'], cv=5, scoring='f1_macro')
  return -1 * scores.mean()

res = gp_minimize(optimize, [['hinge', 'log', 'modified_huber', 'squared_hinge', 'perceptron'], ['l2', 'l1', 'elasticnet'], Real(0.001, 0.3)], n_calls=500, random_state=0)

# Final model
hyper_parameters = res.x
clf = SGDClassifier(loss=hyper_parameters[0], penalty=hyper_parameters[1], alpha=hyper_parameters[2], random_state=0)
scores = cross_val_score(clf, tf.toarray(), df_randomized['target'], cv=5, scoring='f1_macro')
print(scores)