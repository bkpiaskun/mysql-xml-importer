import yaml
import mysql.connector
from lxml import etree
from io import BytesIO
import os
import time

def prepareData(xml_filepath,max_count):
    total_size = len(open(xml_filepath).read().encode('utf-8'))
    with BytesIO(open(xml_filepath).read().encode('utf-8')) as xml_file:
        data = []
        dataChunk = []
        count = 0
        startTime = time.time()
        for event, elem in etree.iterparse(xml_file):
            if count >= max_count:
                insertData(db, table_fields, data, destination_table)
                data = []
                dataChunk = []
                count = 0
                print(str("{:.2f}".format((float(xml_file.tell()/total_size))*100))+"% processed")
            
            if elem.tag == record_name:
                data.append(dataChunk)
                dataChunk = []
                count += 1

            if elem.tag == field_name:
                dataChunk.append(elem.text)
                
            elem.clear()    
        insertData(db, table_fields, data, destination_table)
        progressed = float(xml_file.tell()/total_size)
        processingTime = time.time() - startTime
        if total_size < 1048576:
            print("Total " + str(total_size) + " processed in " + str("{:.2f}".format(processingTime)) + " seconds")
        if total_size > 1048576:
            print("Total " + str("{:.2f}".format(total_size/1048576)) + " megabytes processed in " + str("{:.2f}".format(processingTime)) + " seconds")
            print("Speed: " + str("{:.4f}".format((total_size/1048576)/processingTime)) + " megabytes per second")



def insertData(db, table_fields, data, destination_table):
    mycursor = db.cursor()
    table_fields_template = "("
    table_fields_string = ""
    for field in table_fields:
        table_fields_string += field+", "
        table_fields_template += "%s, "
    table_fields_template = table_fields_template[:-2] + " )"
    sql = "INSERT INTO "+destination_table+" ("+table_fields_string[:-2]+") VALUES "+table_fields_template
    mycursor.executemany(sql, data)
    db.commit()

with open('./import_config.yaml') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

    xml_filepath = config['xml_filepath']
    field_name = config['field_name']
    record_name = config['record_name']

    db_host = config['db_host']
    db_user = config['db_user']
    db_password = config['db_password']
    db_name = config['db_name']
    insert_count = config['insert_count']

    destination_table = config['destination_table']
    table_fields = config['table_fields']

    db = mysql.connector.connect(
    host = db_host,
    user = db_user,
    password = db_password,
    database = db_name
    )

    prepareData(xml_filepath, insert_count)

    
