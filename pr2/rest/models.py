from djongo import models
import uuid
# Create your models here.
class Incident(models.Model):
	id = models.CharField(primary_key=True, max_length=100)
	creation_date      = models.DateField()
	creation_ts        = models.BigIntegerField(null=True, default=None)
	status 			   = models.CharField(max_length=100)
	completion_date    = models.DateField()
	completion_ts      = models.BigIntegerField(null=True, default=None)
	service_request_number  = models.CharField(max_length=100)
	type_of_service_request = models.CharField(max_length=200)
	street_address			= models.CharField(max_length=200)
	zip_code 		   = models.IntegerField()
	x_coordinate	   = models.FloatField()
	y_coordinate	   = models.FloatField()
	ward 			   = models.IntegerField()
	police_district    = models.IntegerField()
	community_area 	   = models.IntegerField()
	longitude		   = models.FloatField()
	latitude		   = models.FloatField()
	location		   = models.CharField(max_length=500)
