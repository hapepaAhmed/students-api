from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
app = FastAPI()
# MongoDB connection setup here atls step2
connection_string='mongodb+srv://habiba:<db_password>@cluster0.brk2poz.mongodb.net/studentsDB?retryWrites=true&w=majority'
Client = MongoClient(connection_string)

db = Client["studentsDB"]
collection = db["messages"]

def addg_message(name, message, age=None):
    doc = {
        "name": name,
        "message": message
    }
    if age is not None:
        doc["age"] = age

    result = collection.insert_one(doc)
    return result.inserted_id




def getg_messages():
    return list(collection.find({}, {"_id": 0}))




def analyzeg():
    pipeline = [
        {
            "$group": {
                "_id": "$name",
                "total_messages": {"$sum": 1}
            }
        },
        {
            "$sort": {"total_messages": -1}
        }
    ]

    return list(collection.aggregate(pipeline))


@app.get("/")
def root():
    return {"status": "running"}

@app.get("/all_messages")
def read_messages():
    return getg_messages()

@app.post("/create_messages")
def create_message():
    addg_message("Hapepa", "Hello MongoDB", 21)
    return {"msg": "inserted"}


# MongoDB connection setup here local step1 

MONGO_URL = "mongodb://localhost:27017"

client = AsyncIOMotorClient(MONGO_URL)
db = client["school_db"]
messages_collection = db["messages"]

# class structer 
class Message(BaseModel):
    message: str
    subject: Optional[str] = None
    class_name: Optional[str] = None
    
    
@app.post("/add_message")
async def add_message(data: Message):
    result = await messages_collection.insert_one(data.dict())
    return {
        "status": "success",
        "id": str(result.inserted_id)
    }

@app.get("/messages")
async def get_messages():
    messages = []
    async for msg in messages_collection.find():
        msg["_id"] = str(msg["_id"])
        messages.append(msg)
    return messages



@app.get("/analyze")
async def analyze(group_by: Optional[str] = None):
    if group_by not in ["subject", "class_name"]:
        raise HTTPException(status_code=400, detail="Invalid group_by")

    pipeline = [
        {"$group": {"_id": f"${group_by}", "count": {"$sum": 1}}}
    ]

    result = []
    async for doc in messages_collection.aggregate(pipeline):
        result.append(doc)

    return result