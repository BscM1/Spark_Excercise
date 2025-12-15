import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_words_in_line <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("count-words-in-line")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("s3a://" + sys.argv[1])

    result = (
        text_file
        .map(lambda line: (line, len(line.split())))
        .reduce(lambda a, b: a if a[1] >= b[1] else b))

    print("Line with max words:")
    print(result[0])
    print("Number of words:", result[1])

    sc.stop()
