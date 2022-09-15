from importlib.abc import ResourceLoader
from lib2to3.pgen2 import driver
import re
from pydantic import BaseModel
from fastapi import FastAPI
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv


load_dotenv()

uri = os.getenv("uri")
user = os.getenv("user")
pwd = os.getenv("pwd")

# print(uri,pwd,user)

def connection():
    driver = GraphDatabase.driver(uri = uri, auth=(user,pwd))
    return driver


app = FastAPI()
@app.get("/")
def default():
    return {"response" : "root "}


@app.get("/query1")
def query1(label):
    driver_neo4j = connection()
    session = driver_neo4j.session()
    q1 = """
    MATCH (a:USER) - [lended: LANDED_LOAN_TO] -> (b:USER) WITH a,COUNT(lended) as cnt, SUM(lended.principle) as sum WHERE cnt > toInteger($c) RETURN a.name,cnt,sum
    """
    x = {"c" : label}
    results = session.run(q1,x)
    return {"response" : [{"Name": row["a.name"], "Count" : row["cnt"] , "SUM" : row["sum"]} for row in results] }



@app.get("/query2")
def query2(label):
    driver_neo4j = connection()
    session = driver_neo4j.session()
    q2 = """
   MATCH (a:USER) - [lended: LANDED_LOAN_TO] -> (b:USER)  WITH a,COUNT(lended) as cnt RETURN a.name,cnt ORDER BY cnt ASC LIMIT toInteger($c)
    """
    x = {"c" : label}
    results = session.run(q2,x)
    return {"response" : [{"Name": row["a.name"], "Count" : row["cnt"] } for row in results] }


@app.get("/query3")
def query3():
    driver_neo4j = connection()
    session = driver_neo4j.session()
    q3 = """
    MATCH p=(a:USER)-[:LANDED_LOAN_TO*1..10]->(b:USER) WHERE NOT (b)-[:LANDED_LOAN_TO]->() RETURN length(p) as Longest_chain ORDER BY length(p) DESC LIMIT 1
    """
    results = session.run(q3)
    return {"response" : [{"Longest Chain": row["Longest_chain"] } for row in results] }


# [{"Name": row["name"], "Count" : row["cnt"] , "SUM" : row["sum"]} for row in results]