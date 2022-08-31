# df-extract-trips
Dataflow that query bulkData positions of vehicles and extract the trips
 
 # How to launch
 ## Excecute un Google Cloud
 To don`t need to deal with auth problems it`s easier execute the script using the CLOUD SHELL
 
 0 - If it is needed a new python library install using `pip3 install firebase-admin`

 1 - Upload the .py to Cloud Shell (wherever you want, in the main folder it`s ok)
 
 2- Execute the command that launch the Dataflow job
 
 ```python obtain_trips.py --input gs://beam_files_mobilidad/dept_data.txt --output gs://beam_files_mobilidad/outputs/part --runner DataflowRunner --project vacio-276411 --temp_location gs://beam_files_mobilidad/tmp```

    -Not using input and output params yet so it's not important
    -Project: Id of your proyect
    -Temp location: Bucket created in your proyect, if it`s not created maybe GCP will autocreate it.
    -Runner: Necesary to specify to use Dataflow
 
 3- If you want to launch the script again remember to delete the .py first and re-upload again
