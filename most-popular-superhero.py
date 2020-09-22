# -*- coding: utf-8 -*-
"""
Created on Sun May 17 17:33:22 2020

@author: sinha
"""

from pyspark import SparkConf, SparkContext

def parseLine(lines):
    fields = lines.split('\"')
    return (int(fields[0]), fields[1])

def findCoOccurence(line):
    elements = line.split()
    hero = int(elements[0])
    return (hero, len(elements)-1)

conf = SparkConf().setMaster("local").setAppName("PopularSuperHero")
sc = SparkContext(conf = conf)

names = sc.textFile("file:///Spark course/Marvel-names.txt")
namesRdd = names.map(parseLine)

data = sc.textFile("file:///Spark course/Marvel-graph.txt")
hero_co_occ = data.map(findCoOccurence)
popular = hero_co_occ.reduceByKey(lambda x, y: x+y)
pairingSorted = popular.map(lambda x: (x[1],x[0]))

mostPopular = pairingSorted.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]
mostPopularName = str(mostPopularName)
print (mostPopularName + " is the most popular superhero with " + str(mostPopular[0]) + " co-appearances.")
