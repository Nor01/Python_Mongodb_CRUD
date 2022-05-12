#pip3 install pymongo[srv]
#pip3 install python-dotenv
from dotenv import load_dotenv,find_dotenv
import os
import pprint
from pymongo import MongoClient, mongo_client

load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://mainor:{password}@maincluster.ujttw.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

client = MongoClient(connection_string)

dbs = client.list_database_names()

test_db = client.test
collections = test_db.list_collection_names()

def insert_test_doc():
    collection = test_db.test #database.collection_name
    test_document = {
        "name":"Tim",
        "type":"Test"
    }

    collection.insert_one(test_document)


production = client.production
person_collection = production.person_collection

def create_documents():
    first_names = ["Tim","Sarah","Jennifer","Jose","Brad","Allen"]
    last_names = ["Cook","Smith","Romero","Jose","Pitt","Stone"]
    ages = [45,56,67,89,23,40]

    docs = []

    for first_name,last_name,age in zip(first_names,last_names,ages):
        doc = {"first_name":first_name,"last_name":last_name,"age":age}

        docs.append(doc)

    person_collection.insert_many(docs)


printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()

    for person in people:
        printer.pprint(person)

def find_tim():
    tim =person_collection.find_one({"first_name":"Tim"})

    printer.pprint(tim)

def count_all_people():
    counter = person_collection.count_documents({})
    print("Number of people", counter)

def get_person_by_id(person_id):

    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    person = person_collection.find_one({"_id":_id})
    printer.pprint(person)

def get_age_range(min_age, max_age):

    query = {"$and":[
                {"age":{"$gte":min_age}},
                {"age":{"$lte":max_age}}
            ]}

    people = person_collection.find(query).sort("age")

    for person in people:
        printer.pprint(person)

def project_columns():
    columns = {"_id":0, "first_name":1, "last_name":1}
    people = person_collection.find({}, columns)

    for person in people:
        printer.pprint(person)

project_columns()
