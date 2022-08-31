from google.cloud import storage
import json
import os



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
    print(linePrepared)
    vehicle = json.loads(linePrepared)
    vehicles_list.append(vehicle)
    
print(vehicles_list[5])