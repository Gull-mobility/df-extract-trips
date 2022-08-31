#Imports bigquery
from google.cloud import bigquery
from pyparsing import unicodeString #For bigquerycredentials
import apache_beam as beam #For apacheBeam
from apache_beam.options.pipeline_options import PipelineOptions #For beam.io.ReadFromBigQuery
#Imports firestore
import firebase_admin
from firebase_admin import firestore
#Import logging in dataflow
import logging
#Import Cloud Storage
from google.cloud import storage
import json
import os


#Conect to bigquery without credentials becouse the execution is inside googleCloud
client = bigquery.Client()

#Options for ReadFromBigQuery
options = PipelineOptions(
    temp_location='gs://colab_temp/temp',
    project ='vacio-276411',
    region='europe-west3')

#Initialize firestore
firebase_admin.initialize_app()
db_firestore = firestore.client()


#CONSTANTS
table_spec_bulkData_b = 'vacio-276411.mainDataset.bulkData_b'
firestore_actual_info = "vehicles_current_b"


#Get data from firestore
def get_firestore_old_situation():
  dict_old_locations = {}

  docs = db_firestore.collection(firestore_actual_info).get()

  #TODO: FInd another idea to convert Firebase collection to python dict
  for doc in docs:
    #print(f'{doc.id} => {doc.to_dict()}')
    dict_old_locations[doc.id] = doc.to_dict()

  print('Vehicles in Firestore: ' +  str(len(dict_old_locations)))
  return dict_old_locations

#Get uiod most recent
def get_last_uoid():
  #DEBUG VARIABLE
  in_debug = False

  query = """
      SELECT uoid, epochTime FROM `vacio-276411.mainDataset.bulkData_b` 
      ORDER BY epochTime DESC
      LIMIT 1
  """
  query_job = client.query(query)  # Make an API request.



  print("The query data:") if in_debug else ''
  for row in query_job:
      print("name={}".format(row[0])) if in_debug else ''
      last_uoid = row[0]

  return last_uoid

def get_list_uoids():
  #DEBUG VARIABLE
  in_debug = False

  query = """
      SELECT uoid, realTime  FROM `vacio-276411.mainDataset.bulkData_b` 
      WHERE DATE(timestamp) = "2022-08-08"
      GROUP BY uoid, realTime
      ORDER BY realTime ASC
      LIMIT 4
  """
  query_job = client.query(query)  # Make an API request.



  print("The query data:") if in_debug else ''
  list_uoids = []
  for row in query_job:
      print("name={}".format(row[0])) if in_debug else ''
      list_uoids.append(row[0])

  return list_uoids


#Prepare to save in firestore
def parseBigQuery(row):
  #DEBUG VARIABLE
  in_debug = False

  print('[FUNC]parseBigQuery') if in_debug else ''
  row['energia'] = float(row['energia'])
  row['latitud'] = float(row['latitud'])
  row['longitud'] = float(row['longitud'])
  row['epochTime'] = float(row['epochTime'])
  row['autonomyValue'] = float(row['autonomyValue'])
  row['seats'] = float(row['seats'])
  print(row) if in_debug else ''
  return row

#Save to firestore
def save_firestore(row, db_firestore):
    


    #DEBUG VARIABLE
    in_debug = True

    print('[FUNC]save_firestore') if in_debug else ''
    print(row) if in_debug else ''
    logging.info(row)
    key = row['matricula']
    print(key) if in_debug else ''
    doc_ref = db_firestore.collection(firestore_actual_info).document(key)
    doc_ref.set(row)
    #Return row to continue with data
    return row

#Check movement
def check_movements(row):
  #DEBUG VARIABLE
  in_debug = False

  print('[FUNC]check_movements') if in_debug else ''
  plate = row['matricula']

  #Check if plate exist in firebase database
  if plate not in dict_old_locations.keys():
    print('[check_movements]New vehicle')
    print(row)
    #NO process
    return row

  new_latitude = row['latitud']
  old_latitude = dict_old_locations[plate]['latitud']

  new_longitude = row['longitud']
  old_longitude = dict_old_locations[plate]['longitud']

  if(new_latitude == old_latitude and new_longitude == old_longitude):
    print('[check_movements] Son iguales') if in_debug else ''
  else:
    print('[check_movements] Son distintas')
    print(new_latitude, old_latitude, new_longitude, old_longitude)

  return row

#Query get new movements
def update_query(last_uoid):
  query = ' '.join(("SELECT * FROM `vacio-276411.mainDataset.bulkData_b`"
          " WHERE uoid = '" + last_uoid +"'",
          "ORDER BY realTime DESC"))
  return query

#Print rows
def print_row(row):
    logging.info(row)
    print(row)
    return row


def FirestoreCompare(row,dict_old_locations):
    #DEBUG VARIABLE
    in_debug = False

    print('[FUNC]check_movements') if in_debug else ''
    plate = row['matricula']

    print('dict_old_locations:')
    print(dict_old_locations)

    #Check if plate exist in firebase database
    if plate not in dict_old_locations.keys():
        print('[check_movements]New vehicle')
        print(row)
        #NO process
        return row

    new_latitude = row['latitud']
    old_latitude = dict_old_locations[plate]['latitud']

    new_longitude = row['longitud']
    old_longitude = dict_old_locations[plate]['longitud']

    if(new_latitude == old_latitude and new_longitude == old_longitude):
        print('[check_movements] Son iguales') if in_debug else ''
    else:
        print('[check_movements] Son distintas')
        print(new_latitude, old_latitude, new_longitude, old_longitude)


    logging.info(row)
    return row


def formatOutput(row):
    row['timestamp'] = str(row['timestamp'])
    return row

###################### SAVE FIRESTORE BLOCK
def getListFromBucket():
    client = storage.Client()

    bucket = client.get_bucket("beam_files_mobilidad")

    blob = bucket.get_blob("outputs/out.txt-00000-of-00001")

    #Download
    blob = blob.download_as_string()
    #Convert bytes to str
    blob = blob.decode('UTF-8')

    vehicles_list = []

    for line in blob.split('}')[:-1]:
        linePrepared = line.replace('\'','"').replace('False','false').replace('True','true') + ' }'
        vehicle = json.loads(linePrepared)
        vehicles_list.append(vehicle)
        
    return vehicles_list


def writeToFirestore(vehicles_list):
    #Start batch
    batch = db_firestore.batch()
    #Set batch info
    for vehicle in vehicles_list:
        nyc_ref = db_firestore.collection(firestore_actual_info).document(vehicle['matricula'])
        batch.set(nyc_ref, vehicle)
    #commit the batch
    batch.commit()
    print('Saved in firestore')
###################### END SAVE FIRESTORE BLOCK


pipeline = beam.Pipeline(options=options)



listUoids = get_list_uoids()

tempUoid = ''
for uoid in listUoids:
  tempUoid = uoid

query = update_query(tempUoid)

side_input = (
  pipeline
  | 'DataFromFirestore' >> beam.Create(get_firestore_old_situation())
)

main_input = (
    pipeline
    | 'QueryTable' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
    | "parseBigQuery" >> beam.Map(parseBigQuery)
)

result = (
    main_input
    | "CalculateMovements" >> beam.Map(FirestoreCompare, dict_old_locations=beam.pvalue.AsDict(side_input) )
    | "print" >> beam.Map(print_row)
    | "Format out to firesbase" >> beam.Map(formatOutput)
    | "Totext" >> beam.io.WriteToText('gs://beam_files_mobilidad/outputs/out.txt', num_shards=1)
)

result = pipeline.run()

result.wait_until_finish()






vehicles_list = getListFromBucket()

print('Number of vehicles: ' + str(len(vehicles_list)) )

writeToFirestore(vehicles_list)