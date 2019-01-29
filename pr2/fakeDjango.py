from pymongo import MongoClient
from faker import Factory
import time
from random import randint
import csv
import json
import uuid


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
	citizen = Citizen()
	citizen.name = fake.first_name() + fake.last_name()
	citizen.address = fake.address()
	citizen.phone_number = fake.phone_number()
	citizen.save()
	incids = getRandomIncidentIds(randint(1, 1000), incidentCollection)
	for inc in incids:
		citizen.upvotes.add(Incident.objects.get(id=inc))
	citizen.save()

def insertBatchCitizens(batchSize, citizenCollection, incidentCollection):
	batch = []
	incidentIds = set()
	for i in range(batchSize):
		citizen = createCitizen(incidentCollection)
		batch.append(citizen)
		incidentIds.update(iid["id"] for iid in citizen["upvotes"])
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