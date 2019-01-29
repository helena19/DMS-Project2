from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from datetime import date
from datetime import datetime
import time
import csv
import json
import uuid
client = MongoClient()
db = client['Chicago311']
collection = db['rest_incident']


def parseDate(data):
	if data.strip() == "":
		return None
	return datetime.strptime(data[0:10],'%Y-%m-%d')

def parseString(data):
	return data

def parseInt(data):
	if data.strip() == "":
		return None
	return int(float(data))

def parseFloat(data):
	if data.strip() == "":
		return None
	return float(data)

def parseJson(data):
	return json.loads(data)

def parseValue(index, data, parser, fields):
	return parser[fields[index]](data)


def parseData(data, parser, fields):
	strId = str(uuid.uuid4())
	resultRow = {
		"_id" : strId,
		"id" : strId,
	}
	for index in range(len(data)):
		resultRow[fields[index]] = parseValue(index, data[index], parser, fields)
	resultRow["creation_ts"] = time.mktime(resultRow["creation_date"].timetuple()) if resultRow["creation_date"] is not None else None
	resultRow["completion_ts"] = time.mktime(resultRow["completion_date"].timetuple()) if resultRow["completion_date"] is not None else None
	return resultRow

def parseCsv(filename, parser, fields):
	with open(filename + ".csv") as file:
		reader = csv.reader(file, delimiter = ",")
		next(reader)
		data = []
		for row in reader:
			data.append(parseData(row, parser, fields))
	return data
import gc
def parseEverything(files, collection):
	for file in files:
		gc.collect()
		print("parsing " + file["filename"])
		data = parseCsv(file["filename"], file["parser"], file["fields"])
		try:
			print("\tmongo insert started")
			collection.insert_many(data)
			print("\tmongo insert completed")
		except BulkWriteError as err:
			print("\tmongo insert failed")
			print(err.details)
			raise err

files = [
	{
		"filename" : "rodent_baiting",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"number_of_premises_baited" : parseInt,
			"number_of_premises_with_garbage" : parseInt,
			"number_of_premises_with_rats" : parseInt,
			"current_activity" : parseString,
			"most_recent_action" : parseString,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "number_of_premises_baited", "number_of_premises_with_garbage", "number_of_premises_with_rats", "current_activity", "most_recent_action", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "latitude", "longitude", "location"]
	},
	{
		"filename" : "abandoned_vehicles",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"licence_plate" : parseString,
			"model" : parseString,
			"color" : parseString,
			"current_activity" : parseString,
			"most_recent_action" : parseString,
			"days_parked" : parseInt,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"ssa" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "licence_plate", "model", "color", "current_activity", "most_recent_action", "days_parked", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "ssa", "latitude", "longitude", "location"]
	},
	{
		"filename" : "alley_lights_out",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "latitude", "longitude", "location"]
	},
	{
		"filename" : "garbage_carts",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"number_of_carts" : parseInt,
			"current_activity" : parseString,
			"most_recent_action" : parseString,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"ssa" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "number_of_carts", "current_activity", "most_recent_action", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "ssa", "latitude", "longitude", "location"]
	},
	{
		"filename" : "gr1",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"graffiti_surface" : parseString,
			"location_on_surface" : parseString,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"ssa" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "graffiti_surface", "location_on_surface", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "ssa", "latitude", "longitude", "location"]
	},
	{
		"filename" : "gr2",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"graffiti_surface" : parseString,
			"location_on_surface" : parseString,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"ssa" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "graffiti_surface", "location_on_surface", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "ssa", "latitude", "longitude", "location"]
	},
	{
		"filename" : "gr3",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"graffiti_surface" : parseString,
			"location_on_surface" : parseString,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"ssa" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "graffiti_surface", "location_on_surface", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "ssa", "latitude", "longitude", "location"]
	},
	{
		"filename" : "gr4",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"graffiti_surface" : parseString,
			"location_on_surface" : parseString,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"ssa" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "graffiti_surface", "location_on_surface", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "ssa", "latitude", "longitude", "location"]
	},
	{
		"filename" : "pot_holes",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"current_activity" : parseString,
			"most_recent_action" : parseString,
			"number_of_potholes_filled_on_block" : parseInt,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"ssa" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "current_activity", "most_recent_action", "number_of_potholes_filled_on_block", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "ssa", "latitude", "longitude", "location"]
	},
	{
		"filename" : "sanitation_code_complaints",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"nature_of_violation" : parseString,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "nature_of_violation", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "latitude", "longitude", "location"]
	},
	{
		"filename" : "street_lights_all_out",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "latitude", "longitude", "location"]
	},
	{
		"filename" : "street_lights_one_out",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "latitude", "longitude", "location"]
	},
	{
		"filename" : "tree_debris",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"debris_location" : parseString,
			"current_activity" : parseString,
			"most_recent_action" : parseString,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "debris_location", "current_activity", "most_recent_action", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "latitude", "longitude", "location"]
	},
	{
		"filename" : "tree_trims",
		"parser" : {
			"creation_date" : parseDate,
			"status" : parseString,
			"completion_date" : parseDate,
			"service_request_number" : parseString,
			"type_of_service_request" : parseString,
			"location_of_trees" : parseString,
			"street_address" : parseString,
			"zip_code" : parseInt,
			"x_coordinate" : parseFloat,
			"y_coordinate" : parseFloat,
			"ward" : parseInt,
			"police_district" : parseInt,
			"community_area" : parseInt,
			"latitude" : parseFloat,
			"longitude" : parseFloat,
			"location" : parseString,
		},
		"fields" : ["creation_date", "status", "completion_date", "service_request_number", "type_of_service_request", "location_of_trees", "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district", "community_area", "latitude", "longitude", "location"]
	},
]

print("reading...")
parseEverything(files, collection)
print("done")
