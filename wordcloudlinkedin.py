from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    connections = fields[4]
    companys = fields[3]
    return (connections,companys)

lines = sc.textFile("c://SparkCourse/Connections.csv")
parsedLines = lines.map(parseLine)

myconnection = parsedLines.collect()
print(myconnection)

res = list(zip(*myconnection))

# Printing modified list
print("Modified list is : " + str(res))

#convert list to string and generate
unique_string=(" ").join(res[0])
wordcloud = WordCloud(background_color='white',
        stopwords= STOPWORDS ,
        max_words=200,
        max_font_size=40,
        scale=3,
        random_state=1).generate(unique_string)
plt.figure(figsize=(15,8))
plt.imshow(wordcloud)
plt.axis("off")
plt.savefig("MyLinkeinconnection"+".png", bbox_inches='tight')
plt.show()
plt.close()
