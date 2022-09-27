# Load and prepare the dataset
from pickletools import read_uint1
import nltk
import random
from nltk.sentiment import SentimentIntensityAnalyzer
# import all nltk libraries
nltk.download('vader_lexicon')
nltk.download('stopwords')
nltk.download('names')
nltk.download('movie_reviews')
nltk.download('averaged_perceptron_tagger')
nltk.download('punkt')

from statistics import mean

from sklearn.naive_bayes import (
    BernoulliNB,
    ComplementNB,
    MultinomialNB,
)
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.neural_network import MLPClassifier
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis

# create analyzer object
sia = SentimentIntensityAnalyzer()


class classify_this():

    def __init__(self):

        # initialize 
        self.calculate_unwanted()

        # initialize once
        self.get_top_words()
        self.get_other_pos_words()

        self.get_features()

        # train models
        self.train()
        self.other_classifiers()

        # verify training
        self.verify()

    # return simple sentiment
    def return_simple_score(self, input):
        scores = [
            sia.polarity_scores(sentence)["compound"]
            for sentence in nltk.sent_tokenize(input)
        ]
        return mean(scores)

    # unwanted words
    def calculate_unwanted(self):
        # create a list of normal words in the english dictionary + names that are neutral and should not be analyzed
        self.unwanted = nltk.corpus.stopwords.words("english")
        self.unwanted.extend([w.lower() for w in nltk.corpus.names.words()])
        self.unwanted.extend(market.lower() for market in ["Bitcoin", "Etherium", "BTC", "ETH", "XRP", "Dogecoin", "ADA", "Kardano", "binance", "bitconnect", "ftx"])

    # this function is a filter function and will return false if the word is in unwanted list and true if its not
    def skip_unwanted(self, pos_tuple):
        word, tag = pos_tuple
        if not word.isalpha() or word in self.unwanted:
            return False
        if tag.startswith("NN"):
            return False
        return True

    # these are the top positive and negative words in the english alphabet, they will emphazise positivity in score in the end
    def get_top_words(self):

        positive_words = [word for word, tag in filter(
            self.skip_unwanted,
            nltk.pos_tag(nltk.corpus.movie_reviews.words(categories=["pos"]))
        )]
        negative_words = [word for word, tag in filter(
            self.skip_unwanted,
            nltk.pos_tag(nltk.corpus.movie_reviews.words(categories=["neg"]))
        )]

        # sort for a frequenzy distribution
        negative_fd = nltk.FreqDist(negative_words)
        positive_fd = nltk.FreqDist(positive_words)

        # common words that are both negative and positive
        common_set = set(positive_fd).intersection(negative_fd)

        # remove all words from pos and neg that are in common_set
        for word in common_set:
            del positive_fd[word]
            del negative_fd[word]

        # make top 100
        self.top_pos_words = {word for word, count in positive_fd.most_common(100)}
        self.top_neg_words = {word for word, count in negative_fd.most_common(100)}

    # another list of positive words
    def get_other_pos_words(self):
        pos_words = ["Adaptable","Adventurous","Amazing","Amiable","Beautiful","Becoming","Beloved","Blessed","Blissful","Brotherly","Calming","Captivating","Charming","Cherished","Comforting","Compelling","Considerable","Credible","Dapper","Darling","Delicious","Delightful","Dependable","Desirable","Dreamy","Durable","Elegant","Empowering","Enchanting","Endearing","Energising","Enjoyable","Enlightening","Exceptional","Fabulous","Fancy","Fantastic","Fashionable","Faultless","Fetching","Flourishing","Formidable","Fulfilling","Funny","Generous","Gifted","Glamorous","Gleaming","Glowing","Godly","Gracious","Gratifying","Happening","Harmonious","Heavenly","Honourable","Ideal","Important","Incredible","Indispensable","Indisputable","Influential","Inspiring","Interesting","Irresistible","Joyful","Jolly","Jovial","Kindly","Kingly","Leading","Legendary","Liberating","Likeable","Lordly","Lovable","Luscious","Luxurious","Magical","Majestic","Memorable","Mesmerizing","Mighty","Miraculous","Motivational","Nifty","Obliging","Optimal","Original","Out of this world","Outgoing","Palatable","Paramount","Peaceful","Peachy","Perfect","Phenomenal","Picturesque","Pleasant","Pleasing","Pleasurable","Positive","Powerful","Praiseworthy","Precious","Prestigious","Prizewinning","Promising","Quality","Radiant","Reasonable","Refreshing","Reliable","Respectable","Revolutionary","Rewarding","Rousing","Saintly","Salubrious","Satisfying","Scrumptious","Sensational","Sexy","Shiny","Showy","Smashing","Soothing","Sought-after","Spectacular","Spiffy","Stimulating","Striking","Stunning","Stupendous","Superb","Supreme","Swanky","Tasteful","Tasty","Terrific","Thrilling","Titillating","Tremendous","Trusty","Ultimate","Unbelievable","Uplifting","Useful","Valuable","Vibrant"]
        self.pos_words = [i.lower() for i in pos_words]


    # create incentive
    def extract_features(self, text):
        features = dict()
        wordcount = 0
        neg_wordcount = 0
        compound_scores = list()
        positive_scores = list()
        negative_scores = list()

        # for every sentence in the text
        for sentence in nltk.sent_tokenize(text):
            # for every word in the sentence
            for word in nltk.word_tokenize(sentence):
                # if the word is in the top positive do +1
                if word.lower() in self.top_pos_words:
                    wordcount += 1
                # if the word is in negative do -1
                elif word.lower() in self.top_neg_words:
                    neg_wordcount += 1

                # if the word is in other positive do +1
                if word.lower() in self.pos_words:
                    wordcount += 1

                # finally add the regular incentive of that word
                if word not in self.unwanted:
                    compound_scores.append(sia.polarity_scores(word)["compound"])
            

            polarity = sia.polarity_scores(sentence)

            compound_scores.append(polarity["compound"])
            positive_scores.append(polarity["pos"])
            negative_scores.append(polarity["neg"])

        # Adding 1 to the final compound score to always have positive numbers
        # since some classifiers you'll use later don't work with negative numbers.
        features["mean_compound"] = mean(compound_scores) + 1
        features["mean_positive"] = mean(positive_scores) + 1
        features["mean_negative"] = mean(negative_scores) + 1
        features["wordcount"] = wordcount
        features["neg_wordcount"] = neg_wordcount

        return features

    # get features
    def get_features(self):
        self.features = [
            (self.extract_features(nltk.corpus.movie_reviews.raw(review)), "pos")
            for review in nltk.corpus.movie_reviews.fileids(categories=["pos"])
        ]
        self.features.extend([
            (self.extract_features(nltk.corpus.movie_reviews.raw(review)), "neg")
            for review in nltk.corpus.movie_reviews.fileids(categories=["neg"])
        ])
        print("got features")



    def train(self):
        # Use 1/4 of the set for training
        train_count = len(self.features) // 4
        random.shuffle(self.features)
        self.custom_classifier = nltk.NaiveBayesClassifier.train(self.features[:train_count])
        self.custom_classifier.show_most_informative_features(10)

        print(nltk.classify.accuracy(self.custom_classifier, self.features[train_count:]))

        print(self.custom_classifier)

    
    def other_classifiers(self):
        self.classifiers = {
            "ComplementNB": ComplementNB(),
            "MultinomialNB": MultinomialNB(),
            "KNeighborsClassifier": KNeighborsClassifier(),
            "RandomForestClassifier": RandomForestClassifier(),
            "LogisticRegression": LogisticRegression(),
            "MLPClassifier": MLPClassifier(max_iter=1000),
            "AdaBoostClassifier": AdaBoostClassifier(),
        }

        # Use 1/4 of the set for training
        self.train_count = len(self.features) // 4
        random.shuffle(self.features)
        for name, sklearn_classifier in self.classifiers.items():
            classifier = nltk.classify.SklearnClassifier(sklearn_classifier)
            classifier.train(self.features[:self.train_count])
            accuracy = nltk.classify.accuracy(classifier, self.features[self.train_count:])
            print(f"{accuracy:.2%} - {name}")


    def verify(self):
        # should be positive
        new_review = "With bitcoin I made it. I finally was able to open up my own bakery, and that only thanks to bitcoin. Honestly guys if you invest smart you can dream big."
        self.test_other_classification(new_review, "pos")
        self.test_simple_classification(new_review, "pos")

    def test_simple_classification(self, text, should_be):
        feature = self.extract_features(text)
        print(f"Should be: {should_be}")
        print(feature)
        print(self.custom_classifier.classify(feature))

    def test_other_classification(self, text, should_be):
        feature = self.extract_features(text)
        for name, sklearn_classifier in self.classifiers.items():
            classifier = nltk.classify.SklearnClassifier(sklearn_classifier)
            classifier.train(self.features[:self.train_count])
            accuracy = nltk.classify.accuracy(classifier, self.features[self.train_count:])
            print(f"{accuracy:.2%} - {name}")
            print(classifier.classify(feature), " should be ", should_be)


    def simple_classifier(self, feature):
        if feature["mean_positive"] > feature["mean_negative"]:
            return "pos"
        else:
            return "neg"


    def classify_other_classifiers(self, feature):
        ls = []
        for name, sklearn_classifier in self.classifiers.items():
            classifier = nltk.classify.SklearnClassifier(sklearn_classifier)
            classifier.train(self.features[:self.train_count])
            accuracy = nltk.classify.accuracy(classifier, self.features[self.train_count:])
            # print(f"{accuracy:.2%} - {name}")
            # print(classifier.classify(feature))
            ls.append(classifier.classify(feature))
        return ls

    # returns 1 if it is positive and 0 if it is negative
    def get_incentive(self, text):
        # combine all classifiers into one and count the simple one 5 times
        outputs = []
        new_review = text
        feature = self.extract_features(new_review)

        # first custom classifier
        outputs.append(str(self.custom_classifier.classify(feature)))
        # then other reviews
        [outputs.append(i) for i in self.classify_other_classifiers(feature)]
        # lastly 5 times simple
        [outputs.append(self.simple_classifier(feature)) for _ in range(0, 5)]
        # print(outputs)

        # calculate if possitive or negative on average
        pos = sum([1 for i in outputs if i == "pos"])
        neg = sum([-1 for i in outputs if i == "neg"])
        outcome = pos+neg

        # finite output
        if outcome >= 0:
            print("POSITIVE")
            return 1
        else:
            print("NEGATIVE")
            return -1


if __name__ == "__main__":
    # test out class
    inc = classify_this()
    inc.return_simple_score("I really love lasagne")
    incentive = inc.get_incentive("I really love lasagne")