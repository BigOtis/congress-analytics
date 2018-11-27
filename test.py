from nltk.stem import WordNetLemmatizer
lemmatizer = WordNetLemmatizer()

print(lemmatizer.lemmatize("determines"))
print(lemmatizer.lemmatize("determined"))
print(lemmatizer.lemmatize("determining"))
print(lemmatizer.lemmatize("cars"))