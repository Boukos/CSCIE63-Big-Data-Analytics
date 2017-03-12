#import /cygdrive/c/Users/mpatnam/Anaconda3/python

import csv
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687",auth=basic_auth("neo4j", "neo4j"))
session = driver.session()

##########################################################################################

#
# upload movies
#
session.run("LOAD CSV WITH HEADERS FROM 'file:///movies.csv' AS line "
"CREATE (m:Movie { id:line.id, title:line.title, year:line.year })")
# verify 
movies = session.run("match(m:Movie) return ID(m) as id, m.title as title, m.year as year")
print("List of movies uploaded...")
for movie in movies:
	print("%s,%s,%s" %(movie["id"], movie["title"], movie["year"]))
print("\n\n")

##########################################################################################

#
# upload actors
#
session.run("LOAD CSV WITH HEADERS FROM 'file:///actors.csv' AS line "
"CREATE (a:Actor { id:line.id,name:line.name })")
# verify 
actors = session.run("match(a:Actor) return ID(a) as id, a.name as name")
print("List of actors uploaded...")
for actor in actors:
	print("%s,%s" %(actor["id"], actor["name"]))
print("\n\n")

##########################################################################################

#
# upload directors
#
session.run("LOAD CSV WITH HEADERS FROM 'file:///directors.csv' AS line "
"CREATE (d:Director { id:line.id,name:line.name })")
# verify 
directors = session.run("match(d:Director) return ID(d) as id, d.name as name")
print("List of directors uploaded...")
for director in directors:
	print("%s,%s" %(director["id"], director["name"]))
print("\n\n")

##########################################################################################

#
# upload actor movie roles
#
session.run("LOAD CSV WITH HEADERS FROM 'file:///movie_actor_roles.csv' AS line "
"MERGE (m:Movie { title:line.title }) "
"ON CREATE SET m.year = line.year "
"MERGE (a:Actor { name:line.actor }) "
"MERGE (a)-[:ACTS_IN { role:line.role }]->(m) ")

# verify 
relations = session.run("match(a:Actor)-[r:ACTS_IN]->(m:Movie) return ID(a) as actorId, ID(m) as movieId, r.role as role")
print("Actor movie roles uploaded...")
for relation in relations:
	print("%s,%s,%s" %(relation["actorId"], relation["movieId"], relation["role"]))
print("\n\n")

##########################################################################################

#
# upload director movie relation
#
session.run("LOAD CSV WITH HEADERS FROM 'file:///dir_relations.csv' AS line "
"MERGE (m:Movie { title:line.title }) "
"ON CREATE SET m.year = line.year "
"MERGE (d:Director { name:line.director }) "
"MERGE (d)-[:DIRECTS]->(m) ")

# verify 
relations = session.run("match(d:Director)-[r:DIRECTS]->(m:Movie) return ID(d) as directorId, ID(m) as movieId")
print("Director movie relations uploaded...")
for relation in relations:
	print("%s,%s" %(relation["directorId"], relation["movieId"]))
print("\n\n")

##########################################################################################

session.close()
