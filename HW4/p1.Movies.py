from pyspark import SparkConf, SparkContext

# analyse a movie review
def analyse(name, reviewFile, stopWords):
    movie = sc.textFile("file://"+reviewFile)

    # Find word counts
    words = movie.flatMap(lambda line: line.split(" "))
    movieWords = words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
    print "----------------------------------------------------------------------\n"
    print "Movie ({0}) #unique words: {1}".format(name, movieWords.count())
    print "First 10 words in the movie: ", movieWords.take(10)

    # remove stop words from the movie words
    movieWordsAfterFilter = movieWords.subtractByKey(stopWords)
    print "----------------------------------------------------------------------\n"
    print "Movie ({0}) #unique words (without stop words): {1}".format(name, movieWordsAfterFilter.count())
    print "Top 10 most frequently used words: ", movieWordsAfterFilter.sortBy(lambda x: x[1], ascending=False).take(10)
    print "----------------------------------------------------------------------\n"

    return movieWordsAfterFilter

# compare movies
def compare(nameA, movieA, nameB, movieB):
    # find words unique in movieA
    uniqueInMovieA = movieA.subtractByKey(movieB)
    print "First 10 words unique in the movie ({0}): {1}".format(nameA, uniqueInMovieA.take(10))
    print "----------------------------------------------------------------------\n"

    # find words unique in movieB
    uniqueInMovieB = movieB.subtractByKey(movieA)
    print "First 10 words unique in the movie ({0}): {1}".format(nameB, uniqueInMovieB.take(10))
    print "----------------------------------------------------------------------\n"

    # find words common to both movies and preserve respective counts
    #commonWords = movieA.intersection(movieB)
    commonWords = movieA.join(movieB)
    fivePerc = int(commonWords.count() * 0.05)
    print "Sample of 5 percent common words to both movies: {0}".format(commonWords.takeSample(False, fivePerc, seed=13))
    print "----------------------------------------------------------------------\n"
    return

# init spark context
conf = SparkConf().setMaster("local").setAppName("Movies")
sc = SparkContext(conf = conf)

# load stop words
stop = sc.textFile("file:///home/cloudera/CSCIE63/HW4/StopWords.txt")
stopWords = stop.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
print "----------------------------------------------------------------------\n"
print "StopWords count: ", stopWords.count()
print "First 10 stop words: ", stopWords.take(10)
#print stopWords.collect()

# load movie A
nameA = "GetOut"
movieA = analyse(nameA, "/home/cloudera/CSCIE63/HW4/GetOutMovie.txt", stopWords)

nameB = "MyLifeAsZucchini"
movieB = analyse(nameB, "/home/cloudera/CSCIE63/HW4/MyLifeAsZucchiniMovie.txt", stopWords)
compare(nameA, movieA, nameB, movieB)

