from pymongo import MongoClient
from faker import Factory
import time
from random import randint
import csv
import json
import uuid
from bson.objectid import ObjectId
import copy

client = MongoClient()
db = client['Chicago311']
collection = db['rest_incident']
collection2 = db['rest_citizen']
collection3 = db['rest_upvote']

def getRandomIncidentIds(number, incidentCollection):
	randomIncidents = incidentCollection.aggregate([{"$sample" : {"size": number}}])
	return [incident for incident in randomIncidents]

def totalIncidents(incidentCollection):
	return incidentCollection.count()

def createCitizen(incidentCollection):
	name = fake.first_name() + ' ' + fake.last_name()
	address = fake.address()
	phone_number = fake.phone_number()
	upvotes = getRandomIncidentIds(randint(1, 1000), incidentCollection)
	strId = str(uuid.uuid4())
	return {
		"_id" : strId,
		"id" : strId,
		'name': name,
		'address': address,
		'phone_number': phone_number,
		'upvotes': upvotes
	}

def insertBatchCitizens(batchSize, citizenCollection, incidentCollection):
	batch = []
	incidentIds = set()
	for i in range(batchSize):
		citizen = createCitizen(incidentCollection)
		batch.append(citizen)
		incidentIds.update([iid["id"] for iid in citizen["upvotes"]])
		for inc in citizen["upvotes"]:
			strId = str(uuid.uuid4())
			element = {
				"_id" : strId,
				"id" : strId,
				"incident_id" : inc["_id"],
				"name" : citizen["name"],
				"phone_number" : citizen["phone_number"],
				"address" : citizen["address"]
			}
			collection3.insert(element)
	print(len(batch))
	print("inserting into database")
	citizenCollection.insert_many(batch)
	print("done")
	return incidentIds

def insertCitizens(incidentCollection, citizenCollection):
	incidentIds = set()
	total_incidents = totalIncidents(incidentCollection)
	while len(incidentIds) < (total_incidents / 2.8):
		print((total_incidents / 2.8) - len(incidentIds))
		newIds = insertBatchCitizens(100, citizenCollection, incidentCollection)
		incidentIds.update(newIds)

if __name__ == '__main__':
    fake = Factory.create()
    insertCitizens(collection, collection2)