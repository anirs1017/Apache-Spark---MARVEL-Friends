# -*- coding: utf-8 -*-
"""
Created on Sun May 17 19:07:30 2020

@author: sinha
"""

from pyspark import SparkContext, SparkConf

def bfsMAP(node):
    characterID = node[0]
    connections, color, distance = node[1]
#    print ('\n\n\n', connections, color, distance, '\n\n\n')
    updatedResults = []
    
    # Check if the current node needs to be expanded
    if (color == 'GRAY'):
        for conn in connections:
            newCharacterID = conn
            newColor = 'GRAY'
            newDistance = distance + 1
            
            if (destCharacter == conn):
                hitCounter.add(1)
            
            newEntry = (newCharacterID, ([], newColor, newDistance))
            updatedResults.append(newEntry)
        
        # Color the current node BLACK as it has been processed
        color = 'BLACK'
   
    updatedNode = (characterID, (connections, color, distance))
    # Save the updated node
    updatedResults.append( updatedNode )    
    return updatedResults
 
def reduceBFS(node1, node2):
    connections1, color1, distance1 = node1
    connections2, color2, distance2 = node2
    
    orig_dist = 9999
    orig_color = color1
    connections = []
    
    # Check if one of the nodes is the original node
    if (len(connections1) > 0):
        connections.extend(connections1)
    if (len(connections2) > 0):
        connections.extend(connections2)
    
    # Keep minimum distance
    orig_dist = min([distance1, distance2])
    
    # Keep darkest color
    if ( (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')) 
          or (color1 == 'GRAY' and color2 == 'BLACK')):
        orig_color = color2
    
    if ( (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')) 
      or (color2 == 'GRAY' and color1 == 'BLACK')):
        orig_color = color1
    
    return (connections, orig_color, orig_dist)

def formBFSConnections(lines):
    fields = lines.split()
    characterID = int(fields[0])
    connections = []
    
    for conn in fields[1:]:
        connections.append(int(conn))
        
    original_color = 'WHITE'
    original_distance = 9999    # Considering INF distance initially
    
    if (characterID == srcCharacter):
        original_color = 'GRAY'
        original_distance = 0
    
    return (characterID, (connections, original_color, original_distance))

def initRDD():
    data = sc.textFile("file:///Spark course/Marvel-graph.txt")
    return data.map(formBFSConnections)


conf = SparkConf().setMaster("local").setAppName("DegreeSeparation")
sc = SparkContext(conf = conf)

# Source Character, 5306 = SpiderMan
srcCharacter = 5306

# Dest Character, 2888 = IRON-MAN
destCharacter = 2888

# Define the accumulator - helps in telling Dest Character is reached
# default value = 0
hitCounter = sc.accumulator(0)

def main():
    iterationRDD = initRDD()
    
    for i in range(10):
        print ("Iteration #", i+1)
        mapped = iterationRDD.flatMap(bfsMAP)
        
        # Evaluating the iterationRDD using mapped.count() 
        print ("\nProcessing " + str(mapped.count()) + " values.")
        
        #Check if accumulator is updated and break if YES, i.e dest reached
        if (hitCounter.value > 0):
            print ("\nReached the destination character. From " + str(hitCounter.value) + " different directions.")
            break
        
        # Otherwise combine the data for each character, keeping 
        # track of darkest color and shortest path
        iterationRDD = mapped.reduceByKey(reduceBFS)

if __name__ == '__main__':
    main()