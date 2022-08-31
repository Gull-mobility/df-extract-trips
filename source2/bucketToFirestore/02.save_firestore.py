#Import Cloud Storage
from google.cloud import storage
import json
import os
#Import Firebase
import firebase_admin
from firebase_admin import firestore


#Initialize firestore
firebase_admin.initialize_app()
db_firestore = firestore.client()

#Constants
firestore_actual_info = "vehicles_current_b"

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
    


vehicles_list = getListFromBucket()

print('Number of vehicles: ' + str(len(vehicles_list)) )

writeToFirestore(vehicles_list)

