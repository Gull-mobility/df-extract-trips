from google.oauth2 import service_account #For credentials
from google.cloud import bigquery #For bigquerycredentials
import apache_beam as beam #For apacheBeam
from apache_beam.options.pipeline_options import PipelineOptions #For beam.io.ReadFromBigQuery
#import math #Para calcular el coseno




#Conect to bigquery without credentials becouse the execution is inside googleCloud
client = bigquery.Client()


#Options for ReadFromBigQuery
options = PipelineOptions(
    temp_location='gs://colab_temp/temp',
    project ='vacio-276411',
    region='europe-west3')

table_spec = 'vacio-276411:mainDataset.bulkData'
table_out = 'vacio-276411:mainDataset.outTemp'
table_trips = 'vacio-276411:mainDataset.trips'

table_schema_out = {
    'fields': [{ 'name': 'servicio', 'type': 'STRING', 'mode': 'NULLABLE'},
               { 'name': 'idVehiculo', 'type': 'STRING', 'mode': 'NULLABLE'},
               { 'name': 'matricula', 'type': 'STRING', 'mode': 'NULLABLE'},
               { 'name': 'energia', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               { 'name': 'latitud', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               { 'name': 'longitud', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               { 'name': 'precio', 'type': 'STRING', 'mode': 'NULLABLE'},
               { 'name': 'tipo', 'type': 'STRING', 'mode': 'NULLABLE'},
               { 'name': 'categoria', 'type': 'STRING', 'mode': 'NULLABLE'},
               { 'name': 'imagen', 'type': 'STRING', 'mode': 'NULLABLE'},
               { 'name': 'uoid', 'type': 'STRING', 'mode': 'REQUIRED'},
               { 'name': 'epochTime', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
               { 'name': 'realTime', 'type': 'STRING', 'mode': 'REQUIRED'},
               { 'name': 'geo', 'type': 'GEOGRAPHY', 'mode': 'REQUIRED'},
               { 'name': 'clusterId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
               { 'name': 'clusterLatitude', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
               { 'name': 'clusterLongitude', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
               { 'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
               { 'name': 'tipoVehiculo', 'type': 'STRING', 'mode': 'REQUIRED'}
            ]
}

table_schema_trip = {
    'fields': [
              { 'name': 'distancia', 'type': 'FLOAT', 'mode': 'NULLABLE'},
              { 'name': 'servicio', 'type': 'STRING', 'mode': 'NULLABLE'},
              { 'name': 'idVehiculo', 'type': 'STRING', 'mode': 'NULLABLE'},
              { 'name': 'matricula', 'type': 'STRING', 'mode': 'NULLABLE'},
              { 'name': 'energia_start', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
              { 'name': 'energia_end', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
              { 'name': 'latitud_start', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
              { 'name': 'latitud_end', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
              { 'name': 'longitud_start', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
              { 'name': 'longitud_end', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
              { 'name': 'precio', 'type': 'STRING', 'mode': 'NULLABLE'},
              { 'name': 'tipo', 'type': 'STRING', 'mode': 'NULLABLE'},
              { 'name': 'categoria', 'type': 'STRING', 'mode': 'NULLABLE'},
              { 'name': 'imagen', 'type': 'STRING', 'mode': 'NULLABLE'},
              { 'name': 'uoid', 'type': 'STRING', 'mode': 'REQUIRED'},
              { 'name': 'epochTime_start', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
              { 'name': 'epochTime_end', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
              { 'name': 'realTime_start', 'type': 'STRING', 'mode': 'REQUIRED'},
              { 'name': 'realTime_end', 'type': 'STRING', 'mode': 'REQUIRED'},
              { 'name': 'geo_start', 'type': 'GEOGRAPHY', 'mode': 'REQUIRED'},
              { 'name': 'geo_end', 'type': 'GEOGRAPHY', 'mode': 'REQUIRED'},
              { 'name': 'clusterId_start', 'type': 'INTEGER', 'mode': 'REQUIRED'},
              { 'name': 'clusterId_end', 'type': 'INTEGER', 'mode': 'REQUIRED'},
              { 'name': 'clusterLatitude_start', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
              { 'name': 'clusterLatitude_end', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
              { 'name': 'clusterLongitude_start', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
              { 'name': 'clusterLongitude_end', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
              { 'name': 'timestamp_start', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
              { 'name': 'timestamp_end', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
              { 'name': 'tipoVehiculo', 'type': 'STRING', 'mode': 'REQUIRED'}
            ]            
}




query = ' '.join(('SELECT * FROM ('
        "SELECT ",
          '*,ROW_NUMBER() OVER(PARTITION BY "a" order by idVehiculo,epochTime)  as tempField',
          'FROM `vacio-276411.mainDataset.bulkData`',
          'WHERE DATE(_PARTITIONTIME) BETWEEN "2021-05-01" AND "2022-05-31"',
      ')WHERE tempField <>1',
      'UNION ALL',
        'SELECT',
          '*,ROW_NUMBER() OVER(PARTITION BY "a" order by idVehiculo,epochTime)+1  as tempField',
          'FROM `vacio-276411.mainDataset.bulkData`',
          'WHERE DATE(_PARTITIONTIME) BETWEEN "2021-05-01" AND "2022-05-31"',
      'ORDER BY tempField,epochTime'))





p2 = beam.Pipeline(options=options)
  
def calcular_distancia(key,param):

  #Import inside function
  import math


  R = 6371  # radius of the earth in km
  lat1 = 0
  lat2 = 0
  lon1 = 0
  lon2 = 0
  distancia = 0 #Default value
  if(len(param)==2 and param[0] and param[0]['latitud']):
    lat1 = float (param[0]['latitud'])
  if(len(param)==2 and param[1] and param[1]['latitud']):
    lat2 = float (param[1]['latitud'])
  if(len(param)==2 and param[0] and param[0]['longitud']):
    lon1 = float (param[0]['longitud'])
  if(len(param)==2 and param[1] and param[1]['longitud']):
    lon2 = float (param[1]['longitud'])
  if(lat1 !=0 and lon1 != 0):
    rad=math.pi/180
    dlat=lat2-lat1
    dlon=lon2-lon1
    RT=6372.795477598
    a=(math.sin(rad*dlat/2))**2 + math.cos(rad*lat1)*math.cos(rad*lat2)*(math.sin(rad*dlon/2))**2
    distancia=2*RT*math.asin(math.sqrt(a))
    
    

    return {'distancia': distancia , 'data':param}

def detectar_movimiento(movement):
  #TODO: Hay veces que movement es Nonetype, hay que saber por que
  try:
    return movement['distancia'] > 0.1
  except:
    return False


max_temperatures = (
    p2
    | 'QueryTable' >> beam.io.ReadFromBigQuery( query=query,use_standard_sql=True)
    | beam.Map(lambda elem: (elem['tempField'], elem))
    | 'Group counts per produce' >> beam.GroupByKey()
    #| beam.MapTuple(lambda key, value: {'distancia': calcular_distancia(value) , 'data':value} )
    | beam.MapTuple( calcular_distancia )
    | 'Filter nomove' >> beam.Filter(detectar_movimiento)
    | beam.Map(lambda elem: {
          'distancia':elem['distancia'],
          'servicio' : elem['data'][0]['servicio'],
          'idVehiculo' : elem['data'][0]['idVehiculo'],
          'matricula' : elem['data'][0]['matricula'],
          'energia_start' : elem['data'][0]['energia'],
          'energia_end' : elem['data'][1]['energia'],
          'latitud_start' : elem['data'][0]['latitud'],
          'latitud_end' : elem['data'][1]['latitud'],
          'longitud_start' : elem['data'][0]['longitud'],
          'longitud_end' : elem['data'][1]['longitud'],
          'precio' : elem['data'][0]['precio'],
          'tipo' : elem['data'][0]['tipo'],
          'categoria' : elem['data'][0]['categoria'],
          'imagen' : elem['data'][0]['imagen'],
          'uoid' : elem['data'][0]['uoid'],
          'epochTime_start' : elem['data'][0]['epochTime'],
          'epochTime_end' : elem['data'][0]['epochTime'],
          'realTime_start' : elem['data'][0]['realTime'],
          'realTime_end' : elem['data'][0]['realTime'],
          'geo_start' : elem['data'][0]['geo'],
          'geo_end' : elem['data'][1]['geo'],
          'clusterId_start' : elem['data'][0]['clusterId'],
          'clusterId_end' : elem['data'][1]['clusterId'],
          'clusterLatitude_start' : elem['data'][0]['clusterLatitude'],
          'clusterLatitude_end' : elem['data'][1]['clusterLatitude'],
          'clusterLongitude_start' : elem['data'][0]['clusterLongitude'],
          'clusterLongitude_end' : elem['data'][1]['clusterLongitude'],
          'timestamp_start' : elem['data'][0]['timestamp'],
          'timestamp_end' : elem['data'][1]['timestamp'],
          'tipoVehiculo' : elem['data'][0]['tipoVehiculo']
        } )
    | beam.io.WriteToBigQuery(
      table_trips,
      schema=table_schema_trip,
      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
)




p2.run()