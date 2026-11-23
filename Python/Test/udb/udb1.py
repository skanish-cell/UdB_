"""
Rate Limiter - FastAPI + MongoDB (Token Bucket)

SETUP
    pip install -r requirements.txt
    # start MongoDB (4.2+ required), e.g. with Docker:
    docker run -d -p 27017:27017 --name mongo mongo:7

RUN
    uvicorn main:app --reload

TEST (burst of 5, refills 1 token/sec)
    for i in $(seq 1 8); do curl -i -s localhost:8000/api/hello | head -n 1; done
    # first 5 -> 200 OK, then 429 Too Many Requests

    curl -H "X-API-Key: alice" localhost:8000/api/hello   # separate bucket per key

DATA MODEL (collection: rate_limits, one document per client)
    {
      "_id": "<api key or IP>",
      "tokens": 3.4,                       # tokens currently available
      "ts": ISODate(...),                  # last update time
      "expireAt": ISODate(...)             # TTL index deletes idle clients
    }
"""

import math
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
CAPACITY = int(os.getenv("RL_CAPACITY", 5))         # max burst size
REFILL_PER_SEC = float(os.getenv("RL_REFILL", 1))   # tokens added per second
TTL_MS = int(math.ceil(CAPACITY / REFILL_PER_SEC) * 2 * 1000)

mongo = AsyncIOMotorClient(MONGO_URL)
limits = mongo["ratelimiter"]["rate_limits"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    # TTL index: MongoDB removes a document once expireAt has passed
    await limits.create_index("expireAt", expireAfterSeconds=0)
    yield


app = FastAPI(title="Token Bucket Rate Limiter (MongoDB)", lifespan=lifespan)


def build_pipeline():
    """
    Aggregation-pipeline update. MongoDB applies it to ONE document
    atomically, so concurrent requests for the same client cannot race.
    $$NOW is the server's clock (same value in every stage).
    """
    return [
        # 1. current tokens / last timestamp (defaults for a brand-new client)
        {"$set": {
            "prev_tokens": {"$ifNull": ["$tokens", CAPACITY]},
            "prev_ts": {"$ifNull": ["$ts", "$$NOW"]},
        }},
        # 2. lazy refill: add elapsed_seconds * rate, capped at capacity
        {"$set": {
            "refilled": {"$min": [
                CAPACITY,
                {"$add": [
                    "$prev_tokens",
                    {"$multiply": [
                        {"$divide": [{"$subtract": ["$$NOW", "$prev_ts"]}, 1000]},
                        REFILL_PER_SEC,
                    ]},
                ]},
            ]},
        }},
        # 3. allowed only if at least 1 token is available
        {"$set": {"allowed": {"$gte": ["$refilled", 1]}}},
        # 4. spend a token if allowed, store new state + expiry
        {"$set": {
            "tokens": {"$cond": ["$allowed", {"$subtract": ["$refilled", 1]}, "$refilled"]},
            "ts": "$$NOW",
            "expireAt": {"$add": ["$$NOW", TTL_MS]},
        }},
        # 5. remove temporary fields
        {"$unset": ["prev_tokens", "prev_ts", "refilled"]},
    ]


async def consume_token(client: str) -> dict:
    for _ in range(2):  # retry once if two first-time requests race on upsert
        try:
            return await limits.find_one_and_update(
                {"_id": client},
                build_pipeline(),
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError:
            continue
    raise HTTPException(status_code=503, detail="Rate limiter busy")


def client_id(request: Request) -> str:
    """Identify client by API key if given, else by IP."""
    return request.headers.get("X-API-Key") or request.client.host


async def rate_limit(request: Request, response: Response):
    doc = await consume_token(client_id(request))
    headers = {
        "X-RateLimit-Limit": str(CAPACITY),
        "X-RateLimit-Remaining": str(max(0, int(doc["tokens"]))),
    }
    if not doc["allowed"]:
        wait = (1 - doc["tokens"]) / REFILL_PER_SEC
        headers["Retry-After"] = str(max(1, math.ceil(wait)))
        raise HTTPException(status_code=429, detail="Too Many Requests", headers=headers)
    response.headers.update(headers)


@app.get("/api/hello", dependencies=[Depends(rate_limit)])
async def hello():
    return {"message": "Hello! Request allowed."}


@app.get("/health")
async def health():
    await mongo.admin.command("ping")
    return {"status": "ok", "db": "mongodb"}
