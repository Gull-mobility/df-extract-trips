#Imports bigquery
from google.cloud import bigquery
#Imports firestore
import firebase_admin
from firebase_admin import firestore
#Import local service account
import os



#Conect to bigquery without credentials becouse the execution is inside googleCloud
client = bigquery.Client()

#Initialize firestore
firebase_admin.initialize_app()
db_firestore = firestore.client()

#CONSTANTS
firestore_actual_info = "vehicles_current_b"




#Get actual positions bigquery for uoid
#Get uiod most recent
def actual_by_id(uoid):

  query = ' '.join(("SELECT * FROM `vacio-276411.mainDataset.bulkData_b`"
            " WHERE uoid = '" + uoid +"'",
            "ORDER BY realTime DESC"))

  query_job = client.query(query)  # Make an API request.

  output = {}
  for row in query_job:
    dictRow = dict(row)

    #Fix some campos that makes problems in fierstore format
    dictRow['energia'] = float(dictRow['energia'])
    dictRow['latitud'] = float(dictRow['latitud'])
    dictRow['longitud'] = float(dictRow['longitud'])
    dictRow['epochTime'] = float(dictRow['epochTime'])
    dictRow['autonomyValue'] = float(dictRow['autonomyValue'])
    dictRow['seats'] = float(dictRow['seats'])

    output[dictRow['matricula']] = dictRow

  return output

#Calculate movements
def calculateMovements(locations,dict_old_locations):

    in_debug = False

    #Para todas las nuevas posiciones comparamos con posicion anterior
    for plate in locations:
        print('[FUNC]check_movements') if in_debug else ''

        print('dict_old_locations:') if in_debug else ''
        print(dict_old_locations) if in_debug else ''

        #Esto ya no haria falta, guardar al final
        #Check if plate exist in firebase database
        if plate not in dict_old_locations.keys():
            print('[check_movements]New vehicle') if in_debug else ''
            print(locations) if in_debug else ''
            #If this vehicule do not exist not continue
        else:

            new_latitude = locations[plate]['latitud']
            old_latitude = dict_old_locations[plate]['latitud']

            new_longitude = locations[plate]['longitud']
            old_longitude = dict_old_locations[plate]['longitud']

            if(new_latitude == old_latitude and new_longitude == old_longitude):
                print('[check_movements] Son iguales') if in_debug else ''
            else:
                print('[check_movements] Son distintas') if in_debug else ''
                print(new_latitude, old_latitude, new_longitude, old_longitude) if in_debug else ''

                #Si son distintas hay que enviar a bigquery - TODO
                
                #Pero tambien hay que actualizar dict_old_location
                dict_old_location[plate] = locations[plate]

    return dict_old_location


#Get old location from firestore
def get_firestore_old_situation():
  dict_old_locations = {}

  docs = db_firestore.collection(firestore_actual_info).get()

  #TODO: FInd another idea to convert Firebase collection to python dict
  for doc in docs:
    #print(f'{doc.id} => {doc.to_dict()}')
    dict_old_locations[doc.id] = doc.to_dict()

  #print('Vehicles in Firestore: ' +  str(len(dict_old_locations)))
  return dict_old_locations

def get_list_uoids():

  #Ahora sin el limit       LIMIT 4
  query = """
      SELECT uoid, realTime  FROM `vacio-276411.mainDataset.bulkData_b` 
      WHERE DATE(timestamp) = "2022-08-08"
      GROUP BY uoid, realTime
      ORDER BY realTime ASC
  """
  query_job = client.query(query)  # Make an API request.

  list_uoids = []
  for row in query_job:
      list_uoids.append(row[0])

  return list_uoids

#Write to firestore
def writeToFirestore(vehicles_list):

    #Prepare 400 items batches
    list_to_batch = []
    in_list = []
    for id,item in enumerate(vehicles_list):
        if(id%400==0):
            list_to_batch.append(in_list.copy())
            in_list = []
        in_list.append(item)

    for items in list_to_batch:
        batch = db_firestore.batch()
        for vehicle_item in items:
            #print(list_to_batch[vehicle_item])
            fst_ref = db_firestore.collection(firestore_actual_info).document(vehicles_list[vehicle_item]['matricula'])
            batch.set(fst_ref, vehicles_list[vehicle_item])
        # Finally commit the batch
        batch.commit()

    """
    #Start batch
    batch = db_firestore.batch()
    #Set batch info
    for vehicle in vehicles_list:
        nyc_ref = db_firestore.collection(firestore_actual_info).document(vehicles_list[vehicle]['matricula'])
        batch.set(nyc_ref, vehicles_list[vehicle])
    #commit the batch
    batch.commit()
    """
    print('Saved in firestore')

#MOVEMENT FUNCTION
##TODO: Renombrar para mayor claridad
def movement(uoid, dict_old_location):
    locations = actual_by_id(uoid)
    dict_old_location = calculateMovements(locations, dict_old_location)
    #Ahora dict_old_location tiene que tener el contenido de location
    return dict_old_location

##GENERAL PROCESS

#Get old location of vehicles
dict_old_location = get_firestore_old_situation()

print('Firestore has a state of ' + str(len(dict_old_location)) + ' vehicles')

#Get all the ids to process
listUoids = get_list_uoids()

#For every id do the process
for count,uoid in enumerate(listUoids):
    print(str(count) + " of " + str(len(listUoids)) )
    dict_old_location = movement(uoid, dict_old_location)

#Write final location
writeToFirestore(dict_old_location)