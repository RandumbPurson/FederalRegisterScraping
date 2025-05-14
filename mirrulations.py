"""Utils for collecting data from the murrilations mirrored dataset of regulations.gov"""
import tempfile
import json
import os

import boto3
from botocore import UNSIGNED, config
import pymongo
from pymongo.errors import OperationFailure

def mongoConnect():
    """Connect to the MongoDB server at the IP specified in the file at `path`
    """
    try:
        IPpath = os.path.join(__file__, os.path.pardir, "mongo", "IP.txt")
        with open(IPpath, "r") as ipFile:
            ip = ipFile.read().strip()
            return pymongo.MongoClient(host=[ip])
    except:
        return pymongo.MongoClient()

unsigned=config.Config(signature_version=UNSIGNED)

s3 = boto3.resource("s3", config=unsigned)
bucket = s3.Bucket("mirrulations")

client = boto3.client('s3', config=unsigned)

def getDockets(agencies=[]):
    """Get a list of dockets in the format `<agency>/<docketId>/`"""
    docket_collection = mongoConnect().mirrulations.raw_dockets
    # inspired by https://stackoverflow.com/questions/54833895/how-to-get-top-level-folders-in-an-s3-bucket-using-boto3
    paginator = client.get_paginator('list_objects')
    result = paginator.paginate(Bucket='mirrulations', Delimiter='/')

    if len(agencies) == 0:
        agencies = [prefix.get("Prefix").rstrip("/") for prefix in result.search('CommonPrefixes') if prefix is not None]
    dockets = []
    existing = 0
    for i, agency in enumerate(agencies):
        print(f"[{i}/{len(agencies)}]({agency})", end=", ")
        result = paginator.paginate(
            Bucket='mirrulations',
            Delimiter='/',
            Prefix=f"{agency}/"
        )
        agency_dockets = [prefix.get("Prefix") for prefix in result.search("CommonPrefixes") if prefix is not None]
        filtered_dockets = list(filter(lambda docket: not docExists(docket.split("/")[1], docket_collection), agency_dockets))
        existing += len(agency_dockets) - len(filtered_dockets)
        dockets.extend(filtered_dockets)
    print(F"Skipping {existing} pre-existing dockets...")
    return dockets

### Helper functions for getting keys
def getSubKeys(path, fullKey=True):
    if fullKey:
        extract = lambda key: key 
    else:
        extract = lambda keystr: keystr.split("/")[-2]
    objects = client.list_objects_v2(
        Bucket="mirrulations", 
        Prefix=path,
        Delimiter="/"
    )
    if "CommonPrefixes" not in objects:
        return []
    return [extract(prefix["Prefix"]) for prefix in objects["CommonPrefixes"]]

def getFileKeys(path):
    return [metadata["Key"] for metadata in client.list_objects(
        Bucket="mirrulations", 
        Prefix=path,
        Delimiter="/"
    )["Contents"]]

### Helper functions for getting lists of content paths
def getContents(base_path, subkey):
    path = base_path + f"{subkey}/"
    keys = getFileKeys(path)
    return keys

def getSubContents(base_path, subkey):
    path = base_path + f"{subkey}/"
    keys = []
    for key in getSubKeys(path, fullKey=True):
        keys.extend(getFileKeys(key))
    return keys

### Functions for getting data from content paths
getFileName = lambda path: path.split("/")[-1].split(".")[0]
def getFileData(paths, dataExtractor=lambda file: json.load(file)):
    data = {getFileName(path).split("_")[0]: [] for path in paths} # init with []
    temp = tempfile.NamedTemporaryFile(delete=False)
    for path in paths:
        bucket.download_file(path, temp.name)
        with open(temp.name, "r") as file:
            comment_key = getFileName(path).split("_")[0]
            data[comment_key].append(dataExtractor(file))

    os.remove(temp.name)
    return data

### Functions for updating structured objects with data
def addUpdate(dict_, key, val):
    if key not in dict_:
        dict_[key] = {}
    dict_[key].update(val)
    
def updateJson(dict_, base_path, key):
    paths = list(filter(
        lambda path: path.split(".")[-1] == "json", 
        getContents(base_path, key)
    ))
    data = getFileData(paths)
    for ID, json_data in data.items():
        addUpdate(dict_, ID, json_data[0])

def updateText(dict_, base_path, key):
    paths = getSubContents(base_path, key)
    data = getFileData(paths, dataExtractor=lambda file: file.read())
    for ID, text in data.items():
        addUpdate(dict_, ID, {"text": text})

docExists = lambda id, collection: collection.count_documents({"id": id}) > 0
### Update a mongoDB collection using a structured object
def updateCollection(obj, db, collection_name):
    collection = db[collection_name]
    docDoesNotExist = lambda data: not docExists(data["id"], collection)
    for ID in obj:
        obj[ID]["id"] = ID
    filtered = list(filter(docDoesNotExist, [data for data in obj.values()]))
    if len(filtered) > 0:
        for doc in filtered:
            try:
                collection.insert_one(doc)
            except OperationFailure as e:
                print("Failed inserting document, retrying without text")
                try:
                    text = doc.pop("text", None)
                    collection.insert_one(doc)
                    db.errors.insert_one({
                        "id": doc["id"],
                        "target": collection_name,
                        "errortype": "partial no_text"
                    })
                except:
                    db.errors.insert_one({
                        "id": doc["id"],
                        "target": collection_name,
                        "errortype": "insert_failed"
                    })

### Main entry point, takes a single docket path and updates collections with data
def storeDocketInfo(docketPath, databaseName="mirrulations"):
    raw_path =  f"raw-data/{docketPath}text-{docketPath.split('/')[-2]}/"
    derived_path = f"derived-data/{docketPath}mirrulations/extracted_txt/"
    raw_fields = getSubKeys(raw_path, fullKey=False)
    der_fields = getSubKeys(derived_path, fullKey=False)
    bson_comments = {}
    bson_documents = {}
    bson_docket = {}
    if "comments" in raw_fields:
        updateJson(bson_comments, raw_path, "comments")
    if "documents" in raw_fields:
        updateJson(bson_documents, raw_path, "documents")
    if "docket" in raw_fields:
        updateJson(bson_docket, raw_path, "docket")
    
    if "comments_extracted_text" in der_fields:
        updateText(bson_comments, derived_path, "comments_extracted_text")
    if "documents_extracted_text" in der_fields:
        updateText(bson_documents, derived_path, "documents_extracted_text")

    db = mongoConnect()[databaseName]
    updateCollection(bson_comments, db, "raw_comments")
    updateCollection(bson_documents, db, "raw_documents")
    updateCollection(bson_docket, db, "raw_dockets")
    print(f"[✓] {docketPath.split('/')[1]}")
