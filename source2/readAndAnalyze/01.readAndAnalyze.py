#IMPORTS
from google.oauth2 import service_account #For credentials
from google.cloud import bigquery #For bigquerycredentials
import apache_beam as beam #For apacheBeam
from apache_beam.options.pipeline_options import PipelineOptions #For beam.io.ReadFromBigQuery
#IMPORTS FIRESTORE
import firebase_admin
from firebase_admin import firestore
#Import logging in dataflow
import logging
#Import datastore write
#from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
#from apache_beam.io.gcp.datastore.v1new.types import Entity
#import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO
#from apache_beam.io.gcp.datastore.v1.datastoreio
#from apache_beam.io.gcp.datastore.v1new.types import Query
#from google.cloud.datastore.key import Key
#from googledatastore import helper as datastore_helper
#from google.cloud.grpc.datastore.v1 import entity_pb2 #pip3 install grpc-google-cloud-datastore-v1
from apache_beam.io.fileio import WriteToFiles

import json

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

#Main execution
#First update old situation
#get_firestore_old_situation()
#Get last uoid
last_uoid =  get_last_uoid()
#Update query with last uoid
last_uoid = '27f342be-174a-4278-88aa-8fbb8b9ce0e6'
last_uoid = 'a2e360d5-e5b9-4cac-8893-ac9d911a94f3'
last_uoid = '9719a677-68ee-46b8-befd-1b15c62c5012'
query = update_query(last_uoid)

#Check movements necesita tambien dict_old_locations pero hay que pasarselo desde algun lado

pipeline = beam.Pipeline(options=options)

#TODO: Pending add this:     #| "SaveInFirestore" >> beam.Map(save_firestore)

"""
class FirestoreCompare(beam.DoFn):
  def process(self, row,dict_old_locations):
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
"""
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

"""
class CreateEntities(beam.DoFn):

    def process(self, element):
        entity = Entity()
        entity.set_properties(element)
        #document_id = element['matricula']
        return [entity]
        #return [(document_id, element)]
"""

"""
class FirestoreWriteDoFn(beam.DoFn):
  def __init__(self):
    super(FirestoreWriteDoFn, self).__init__()

  def start_bundle(self):
    import google.cloud 
    self.db = google.cloud.firestore.Client(project='vacio-276411')

  def process(self, element):
    fb_data = {
      'topics': element.get('page_keywords').split(','),
      'title': element.get('page_title')
    }
    logging.info('Inserting into Firebase: %s', fb_data)
    fb_doc = self.db.document('totallyNotBigtable', element.get('key'))
    result = fb_doc.create(fb_data)
    yield result
"""

class FirestoreWriteDoFn(beam.DoFn):
    MAX_DOCUMENTS = 200
 
    def __init__(self, project, collection):
        self._project = project
        self._collection = collection
        beam.DoFn.__init__(self)
        #super(FirestoreWriteDoFn, self).__init__()
 
    def start_bundle(self):
        self._mutations = []
 
    def finish_bundle(self):
        if self._mutations:
            self._flush_batch()
 
    def process(self, element, *args, **kwargs):
        self._mutations.append(element)
        if len(self._mutations) > self.MAX_DOCUMENTS:
            self._flush_batch()
 
    def _flush_batch(self):
        db = firestore.Client(project=self._project)
        batch = db.batch()
        for mutation in self._mutations:
            if len(mutation) == 1:
                # autogenerate document_id
                ref = db.collection(self._collection).document()
                batch.set(ref, mutation)
            else:
                ref = db.collection(self._collection).document(mutation[0])
                batch.set(ref, mutation[1])
        r = batch.commit()
        logging.debug(r)
        self._mutations = []


class CreateEntities(beam.DoFn):
    """Creates Datastore entity"""
 
    def process(self, element):
        document_id = element['matricula']
        return [(document_id, element)]


def convert(row):
    return 'blabla'
    #return json.dumps(row)

def formatOutput(row):
    row['timestamp'] = str(row['timestamp'])
    return row

side_input = (
    pipeline
    | 'DataFromFirestore' >> beam.Create(get_firestore_old_situation())
    #| "print" >> beam.Map(print_row)
)

main_input = (
    pipeline
    | 'QueryTable' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
    | "parseBigQuery" >> beam.Map(parseBigQuery)
)

result = (
    main_input
    | "CalculateMovements" >> beam.Map(FirestoreCompare, dict_old_locations=beam.pvalue.AsDict(side_input) )
    #| "CalculateMovements" >> beam.ParDo(FirestoreCompare, dict_old_locations=beam.pvalue.AsDict(side_input) )
    | "print" >> beam.Map(print_row)
    #| "SaveInFirestore" >> beam.Map(save_firestore, db_firestore)
    #| 'Create entities' >> beam.ParDo(CreateEntities())
    #| 'Write entities into Firestore' >> WriteToDatastore(project='vacio-276411')
    #| 'WriteFirebase' >> beam.ParDo(FirestoreWriteDoFn('vacio-276411', 'Productsxx'))
    | "Format out to firesbase" >> beam.Map(formatOutput)
    | "Totext" >> beam.io.WriteToText('gs://beam_files_mobilidad/outputs/out.txt', num_shards=1)
)

"""
| 'Map' >> beam.Map(convert)
| "print2" >> beam.Map(print_row)
|  'OutputToFile' >> WriteToFiles(path='gs://beam_files_mobilidad/outputs', file_naming='salida.txt')
"""

"""
vehicles_location = (
    pipeline
    | 'QueryTable' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
    | "FormatToFirestore" >> beam.Map(prepare_to_save_in_firestore)
    #TODO: Temp coment
    #| "SaveInFirestore" >> beam.Map(save_firestore)
    | "CalculateMovements" >> beam.Map(check_movements)
    #| "print" >> beam.Map(print_row)
)
"""



result = pipeline.run()

"""
result.wait_until_finish()

# print the output
print(result)
"""