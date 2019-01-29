from django.core.serializers.json import DjangoJSONEncoder

from django.http import HttpResponse

from rest_framework import viewsets, generics

from .models import Incident

import datetime
import time
import collections

from django.db.models import Count
from django.db.models import Avg, Sum
from django.db.models import F, Q
from faker import Factory

from datetime import datetime as dt

import json

import uuid

import pymongo

service_request_types = ["Graffiti Removal", "Street Lights - All/Out", "Pot Hole in Street", "Garbage Cart Black Maintenance/Replacement", "Street Light - 1/Out", "Alley Light Out", "Abandoned Vehicle Complaint", "Rodent Baiting/Rat Complaint", "Sanitation Code Violation", "Tree Trim", "Tree Debris"]
must_have = ["creation_date", "creation_ts", "status", "completion_date", "completion_ts", "service_request_number", "type_of_service_request", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "longitude", "latitude", "location", "graffiti_surface", "location_on_surface", "ssa", "current_activity", "most_recent_action", "number_of_potholes_filled_on_block", "nature_of_violation",  "community_area", "number_of_premises_baited", "number_of_premises_with_garbage", "number_of_premises_with_rats", "licence_plate", "model", "color", "days_parked", "number_of_carts", "nature_of_violation", "debris_location", "location_of_trees"]

def query1(request):
	fromDate = request.GET.get('from_date')
	toDate = request.GET.get('to_date')

	result = [['type_of_service_request', 'count']]

	data = Incident.objects.filter(creation_date__gte=fromDate, creation_date__lte=toDate).values('type_of_service_request').annotate(count=Count('type_of_service_request')).order_by('-count')
	for row in data:
		print(row)
		result.append([row['type_of_service_request'], row['count']])

	# client = pymongo.MongoClient("mongodb://localhost:27017/")
	# db = client['Chicago311']
	# collection = db['rest_incident']

	# fromDate_ts = time.mktime(dt.strptime(fromDate, '%Y-%m-%d').timetuple()) + 28800
	# toDate_ts = time.mktime(dt.strptime(toDate, '%Y-%m-%d').timetuple()) + 28800

	# data = collection.aggregate(
	# 	pipeline = [
	# 	{
	# 		"$match" : {
	# 			"$and" : [{"creation_ts" : {"$gte" : fromDate_ts} }, {"creation_ts" : {"$lte" : toDate_ts} }]
	# 		}
	# 	},
	# 	{ 
	# 		"$group" : { 
	# 			"_id" : "$type_of_service_request", 
	# 			"count" : { "$sum" :  1 } 
	# 		},

	# 	},
	# 	{
	# 		"$sort" : {
	# 			"count": -1
	# 		}
	# 	},
	# ]
	# )

	# for row in data:
	# 	result.append([row['_id'], row['count']])
	
	return HttpResponse(json.dumps(result));

def query2(request):
	fromDate = request.GET.get('from_date')
	toDate = request.GET.get('to_date')
	requestType = request.GET.get('request_type')
	print(fromDate)
	print(toDate)
	print(requestType)

	data = Incident.objects.filter(type_of_service_request=requestType, creation_date__gte=fromDate, creation_date__lte=toDate).values('creation_date').annotate(count=Count('creation_date')).order_by('-count')
	result = [['creation_date', 'count']]
	for row in data:
		print(row)
		result.append([row['creation_date'], row['count']])
	return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder))

def query3(request):
	date = request.GET.get('date')

	data = Incident.objects.filter(creation_date=date).values('zip_code', 'type_of_service_request').annotate(count=Count('id')).order_by('zip_code', '-count')
	result = [['zip_code', 'type_of_service_request']]
	counter = {}
	for row in data:
		if not(row['zip_code']) in counter:
			counter[row['zip_code']] = []
		if len(counter[row['zip_code']]) == 3:
			continue
		counter[row['zip_code']].append(row['type_of_service_request'])
		result.append([row['zip_code'], row['type_of_service_request']])
		
	return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));

def query4(request):
	requestType = request.GET.get('request_type')

	data = Incident.objects.filter(type_of_service_request=requestType).values('ward').annotate(count=Count('ward')).order_by('count')
	result = [data[0], data[1], data[2]]
		
	return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));

def query5(request):
	fromDate = request.GET.get('from_date')
	toDate = request.GET.get('to_date')

	data = Incident.objects.filter(~Q(status='Open'), creation_date__gte=fromDate, creation_date__lte=toDate).values('type_of_service_request').annotate(avg_creation=Avg('creation_ts'), avg_completion=Avg('completion_ts')).order_by('type_of_service_request', 'avg_creation', 'avg_completion')
	result = [['type_of_service_request', 'avg_completion_days']]	
	for row in data:
		result.append([row['type_of_service_request'], str(dt.fromtimestamp(int(row['avg_completion'])) - dt.fromtimestamp(int(row['avg_creation'])))])
	return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));

def query6(request):
	client = pymongo.MongoClient("mongodb://localhost:27017/")
	db = client['Chicago311']
	collection = db['rest_incident']

	date = request.GET.get('date')
	x = request.GET.get('x')
	y = request.GET.get('y')
	z = request.GET.get('z')
	w = request.GET.get('w')

	data = collection.aggregate(pipeline=[
		{
			"$project" : {
				"coordinates" : { "$setUnion": ["$latitude", "$longitude"]},
				"type_of_service_request" : "$type_of_service_request",
			},
		},
		{
			"$match" : {
				"creation_date" : date,	
			}
		},
		{
			"coordinates" : {
				"$geoWithin" : {
					"$box" : [[x,y],[z,w]]
				}
			}
		},
		{ 
			"$group" : { 
				"_id" : "$type_of_service_request", 
				"count" : { "$sum" :  1 } 
			},

		},
		{
			"$sort" : {
				"count": -1
			}
		},
		{
			"$limit" : 1
		}
	]
	)

	for row in data:
		result.append(row)
	return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));

def query7(request):
	dateString = request.GET.get('date')
	date = int(time.mktime(datetime.datetime.strptime(dateString,'%Y-%m-%d').timetuple())) + 28800

	client = pymongo.MongoClient("mongodb://localhost:27017/")
	db = client['Chicago311']
	collection = db['rest_citizen']

	data = collection.aggregate(pipeline=[
		{
			"$unwind" : "$upvotes"
		},
		{ 
			"$match" : {
				"upvotes.creation_ts": date,
			}
		},
		{ 
			"$group" : { 
				"_id" : "$upvotes._id", 
				"count" : { "$sum" :  1 } 
			},

		},
		{
			"$sort" : {
				"count": -1
			}
		}
	],
	allowDiskUse = True
	)

	result = []
	count = 0
	for row in data:
		if (count < 50):
			result.append(row)
		else:
			break
		count = count + 1
	return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));

def query8(request):
	client = pymongo.MongoClient("mongodb://localhost:27017/")
	db = client['Chicago311']
	collection = db['rest_citizen']

	data = collection.aggregate(pipeline=[
		{
			"$unwind" : "$upvotes"
		},
		{
			"$group": {
				"_id" : { 
					"id" : "$_id",
					"name" : "$name"
				},
				"count" : { "$sum" : 1}
			},
		},
		{
			"$sort" : {
				"count": -1
			}
		},
		{
			"$limit": 50
		}
		
	],
	allowDiskUse = True
	)

	result = []
	for row in data:
		result.append(row)
	return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));

def query9(request):
	client = pymongo.MongoClient("mongodb://localhost:27017/")
	db = client['Chicago311']
	collection = db['rest_citizen']

	data = collection.aggregate(pipeline=[
		{
			"$unwind" : "$upvotes"
		},
		{
			"$group" : {
				"_id" : {"id" : "$_id" , "ward": "$upvotes.ward"},
			}
		},
	]
	)

	result = []
	for row in data:
		result.append(row["_id"]["id"])
	result.sort()
	counter = collections.Counter(result)
	result = []
	count = 0
	for row in counter:
		print(row)
		if (count < 50):
			result.append(row)
		else:
			break
		count = count + 1
	final_result = []
	for citizen_id in result:
		data = collection.find_one({"_id" : citizen_id})
		final_result.append(data["name"])
	return HttpResponse(json.dumps(final_result, cls=DjangoJSONEncoder));

def query10(request):
	client = pymongo.MongoClient("mongodb://localhost:27017/")
	db = client['Chicago311']
	collection = db['rest_citizen']

	data = collection.aggregate(pipeline=[
		{ 
			"$group": {
				"_id": { "phone_number": "$phone_number" },
				"count": { "$sum": 1 } 
			} 
		}, 
		{ 
			"$match": { 
				"count": { "$gte": 2 } 
			}
		},
		{
			"$sort" : { "count" : -1} 
		},
	]
	)

	result = []
	count = 0
	for row in data:
		result.append(row["_id"]["phone_number"])

	final_result = []
	for row in result:
		collection_upvotes = db['rest_upvote']
		data = collection_upvotes.find_one({"phone_number" : row}) 
		final_result.append(data["incident_id"])

	return HttpResponse(json.dumps(final_result, cls=DjangoJSONEncoder));

def query11(request):
	name = request.GET.get('name')

	client = pymongo.MongoClient("mongodb://localhost:27017/")
	db = client['Chicago311']
	collection = db['rest_citizen']

	data = collection.aggregate(pipeline=[
		{
			"$unwind" : "$upvotes"
		},
		{ 
			"$match" : {
				"name": name,
			}
		},
		{
			"$group": {
				"_id" : "$upvotes.ward",
			},
		},
	],
	allowDiskUse = True
	)

	result = []
	for row in data:
		result.append(row)
	return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));

def insert(request):
	incident = request.GET.get('incident')
	jsonIncident = json.loads(incident)

	client = pymongo.MongoClient("mongodb://localhost:27017/")
	db = client['Chicago311']
	collection = db['rest_incident']
	collection_error = db['insertion_errors']
	
	service_request_type = jsonIncident['type_of_service_request']

	fields = list(jsonIncident.keys())
	for field in fields:
		if field not in must_have:
			collection_error.insert_one(jsonIncident)
			result = ["Operation failed, unknown field"]
			return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));

	if service_request_type not in service_request_types:
		collection_error.insert_one(jsonIncident)
		result = ["Operation failed, there is no such type of incident"]
		return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));

	if ("creation_date" not in fields) or ("creation_ts" not in fields) or ("completion_date" not in fields) or ("completion_ts" not in fields) or ("zip_code" not in fields) or ("ward" not in fields): 
		collection_error.insert_one(jsonIncident)
		result = ["Operation failed, must-have fields missing"]
		return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));	

	strId = str(uuid.uuid4())
	jsonIncident = {
		"_id" : strId,
		"id" : strId,
	}
	collection.insert_one(jsonIncident)

	result = ["Incident reported successfully"]
	return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));

def citizen(request):
	name = request.GET.get('name')
	address = request.GET.get('address')
	phone_number = request.GET.get('phone_number')
	upvotes = []

	client = pymongo.MongoClient("mongodb://localhost:27017/")
	db = client['Chicago311']
	citizen_collection  = db['rest_citizen']
	citizen = citizen_collection.find_one({"name" : name})
	if citizen != None:
		response_str = "Operation failed, citizen with name " + name + " already exists"
		result = [response_str]
		return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder))

	strId = str(uuid.uuid4())
	jsonCitizen = {
		"_id" : strId,
		"id" : strId,
		"name" : name,
		"address" : address,
		"phone_number" : phone_number,
		"upvotes" : upvotes
	}
	citizen_collection.insert_one(jsonCitizen)
	response_str = "Citizen " + name + " created successfully"
	result = [response_str]
	return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));	


def upvote(request):
	name = request.GET.get('name')
	incident_id = request.GET.get('incident_id')

	client = pymongo.MongoClient("mongodb://localhost:27017/")
	db = client['Chicago311']
	incident_collection = db['rest_incident']
	citizen_collection  = db['rest_citizen']
	upvote_collection   = db['rest_upvote']

	result = []

	citizen = citizen_collection.find_one({"name" : name})
	
	if citizen is None:
		response_str = "Operation failed, no such citizen"
		result = [response_str]
		return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder))
	
	upvote = upvote_collection.find({ "$and": [ { "name": { "$eq": name } }, { "incident_id": { "$eq": incident_id } } ] })
	counter = 0
	for up in upvote:
		counter = counter + 1
	
	if counter == 1000:
		response_str = "Operation failed, citizen " + name + " has 1000 votes already"
		result = [response_str]
		return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder))

	if counter != 0:
		response_str = "Operation failed, there is already an upvote from " + name + " for incident " + incident_id
		result = [response_str]
		return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder))

	incident = incident_collection.find_one({"_id" : incident_id})
	print(incident)

	strId = str(uuid.uuid4())
	new_rest_upvote = {
		"_id" : strId,
		"id" : strId,
		"incident_id" : incident_id,
		"name" : citizen["name"],
		"phone_number": citizen["phone_number"],
		"address" : citizen["address"]
	}
	citizen_collection.update({"_id" : citizen["_id"]}, {"$push": {"upvotes" : incident}})
	upvote_collection.insert_one(new_rest_upvote)

	response_str = "Citizen " + name + " successfully upvoted incident " + incident_id 
	result = [response_str]
	return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder));
