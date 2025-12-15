import sys
from pyspark import SparkContext, SparkConf

def clean_word(word):
    word = word.rstrip('.,')
    if word.isalpha():
        return word
    return None

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: longest-word <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("longest-word")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("s3a://" + sys.argv[1])

    words = (
        text_file
        .flatMap(lambda line: line.split())
        .map(clean_word)
        .filter(lambda w: w is not None))

    longest = (
        words
        .map(lambda w: (w, len(w)))
        .reduce(lambda a, b: a if a[1] >= b[1] else b))

    print("Longest word:", longest[0])
    print("Length:", longest[1])

    sc.stop()
