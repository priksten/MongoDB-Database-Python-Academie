import csv
import pymongo
import sys

# Functies tbv basic database operaties, d.w.z. aanmaken/verwijderen/aanpassen van databases en hun inhoud

def create_database(database_name):
    """
    Deze functie maakt binnen het cluster een database (database_name) aan (als deze nog niet bestaat)
    """
    client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
    mydb = client[database_name]

def create_collection(database_name, collection_name):
    """
    Deze functie maakt binnen een database (database_name) een collectie (collection_name) aan
    """
    client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
    mydb = client[database_name]
    mycol = mydb[collection_name]

def show_databases():
    """Deze functie laat alle databases zien in mijn cluster"""
    client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
    print(client.list_database_names())

def show_collections_in_database(database_name):
    """
    Deze functies laat een lijst van collecties binnen de database (database_name) zien    
    """
    client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
    mydb = client[database_name]
    print(mydb.list_collection_names())

def get_data(database_name, collection_name, query = ''): 
    """
    Deze functie vraagt op basis van een query informatie op uit collectie (collection_name) uit database (database_name).
    Vervolgens worden deze in de terminal afgdrukt.
    Het derde argument, query, is optioneel
    """
    client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
    mydb = client[database_name]
    mycol = mydb[collection_name]

    if query:
        mydoc = mycol.find(query)
        for x in mydoc:
            print(x)
    else:
        mydoc = mycol.find()
        for x in mydoc:
            print(x)

def insert_document(database_name, collection_name, data_dict):
    """
    Voegt documenten toe aan een bepaalde collectie
    """
    client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
    mydb = client[database_name]
    mycol = mydb[collection_name]

    x = mycol.insert_one(data_dict)

def insert_multiple_documents(database_name, collection_name, data_list):
    """
    Voegt een lijst van documenten aan een bepaalde collectie
    """
    client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
    mydb = client[database_name]
    mycol = mydb[collection_name]

    x = mycol.insert_many(data_list)

def update_documents(database_name, collection_name, query, new_values):
    """
    Wijzigt het eerste document in de collectie die aan de query voldoet
    """
    client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
    mydb = client[database_name]
    mycol = mydb[collection_name]

    mycol.update_one(query, new_values)


def update_all_documents(database_name, collection_name, query, new_values):
    """
    Wijzigt alle documenten in de collectie die aan de query voldoen
    """
    client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
    mydb = client[database_name]
    mycol = mydb[collection_name]    
    
    x = mycol.update_many(query, new_values)

def delete_document(database_name, collection_name, query):
    """
    Deze functie verwijdert het eerste document in de collectie die voldoet aan query
    """
    client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
    mydb = client[database_name]
    mycol = mydb[collection_name] 

    mycol.delete_one(query)

def delete_all_documents(database_name, collection_name, query):
    """
    Deze functie verwijdert alle documenten in de collectie die voldoen aan query
    """
    client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
    mydb = client[database_name]
    mycol = mydb[collection_name] 

    mycol.delete_many(query)

def delete_collection(database_name, collection_name):
    """
    Deze functie verwijdert een collectie
    """
    client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
    mydb = client[database_name]
    mycol = mydb[collection_name] 

    mycol.drop()

# Functies tbv data lezen uit csv en importeren in mongoDB database.
# Het is hierbij nodig dat de data uit het csv-bestand in een dictionary wordt ingelezen. 
# Bedenk dat het voor mongoDB nodig is dat de documenten in de database worden gemaakt aan de hand van Python-dictionaries 

def read_csv(file, encode = "UTF-8"):
    """
    Deze functie accepteert als invoer een csv-bestand en retourneert een lijst waarin ieder element een lijst is met persoonseigenschappen"
    """
    with open(file, newline='', encoding = encode) as f:
        reader = csv.reader(f, delimiter = ';')
        person_list = []
        for i, row in enumerate(reader):
            if i == 0:
                # Verwijder BOM van de eerste cel in de eerste rij, indien aanwezig
                row[0] = row[0].lstrip('\ufeff')
            person_list.append(row)
    
    return person_list

def create_documents(person_list):
    """
    Deze functie gebruikt de informatie uit person_list (zoals geretourneerd door de functie read_csv) en maakt voor elke student een dictionary aan
    """
    # keys_list = person_list[0]
    keys_list = person_list[0]
    print(len(person_list))
    document_list = []

    for person_number in range(1, len(person_list)):
        values_list = []
        person = {}

        # in technische voorraad 

        for i in range(0, len(keys_list)):
            values_list.append(person_list[person_number][i])

        for index in range(0, len(keys_list)):
            person[keys_list[index]] = values_list[index]

        document_list.append(person)      
    
    return document_list 

# Functies tbv demonstratie
def demonstrate_delete():
    delete_collection("Python_Academie", "Studenten")
    delete_collection("Python_Academie", "Docenten")
    delete_collection("Technische_Artikelen", "Voorraad")

def create_student_database():
    create_collection("Python_Academie", "Studenten")
    studenten = read_csv("Student.csv")
    students_list = create_documents(studenten)
    insert_multiple_documents("Python_Academie", "Studenten", students_list)

def create_teacher_database():
    create_collection("Python_Academie", "Docenten")
    docenten = read_csv("Docent.csv")
    teachers_list = create_documents(docenten)
    insert_multiple_documents("Python_Academie", "Docenten", teachers_list)

def create_voorraad_database():
    create_collection("Technische_Artikelen", "Voorraad")
    artikelen = read_csv("technische_voorraad.csv")
    artikelen_lijst = create_documents(artikelen)
    insert_multiple_documents("Technische_Artikelen", "Voorraad", artikelen_lijst)

# -----------------------------------------------------------------------------------------------------------------------------------------------------------
# Voorbereiding demonstratie (verwijderen collecties in Python_Academie en Technische_Artikelen):

demonstrate_delete()


# -----------------------------------------------------------------------------------------------------------------------------------------------------------
# Demonstratie 1: Python Academie

# create_student_database()
# create_teacher_database()

#-----------------------------------------------------------------------------------------------------------------------------------------------------------
# Demonstratie 2: Technische Artikelen

# create_voorraad_database()

#------------------------------------------------------------------------------------------------------------------------------------------------------------


# client = pymongo.MongoClient("mongodb+srv://peterriksten:4OH0LqlyVxllTtab@firstcluster.hbxzqxu.mongodb.net/")
# mydb = client["Technische_Artikelen"]
# mycol = mydb["Voorraad"]
# document_count = mycol.count_documents({})
# print(document_count)