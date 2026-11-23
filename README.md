# Rate Limiter Using Python, FastAPI and MongoDB

## Overview

This project implements an API rate limiter using Python, FastAPI, and MongoDB. It regulates the number of requests a client can make by controlling the availability of tokens within a continuously replenished bucket.

## Algorithm

Token Bucket Algorithm

## Data Model

MongoDB Collection (`rate_limits`)

Each client is assigned a document containing:
- Client ID (API key or IP address)
- Available tokens
- Last update timestamp
- Expiration timestamp

## Configuration

- Bucket capacity: 5 tokens
- Refill rate: 1 token per second
- Database: MongoDB
- API Framework: FastAPI
- Language: Python

## Working Principle

The system identifies each client using an API key provided through the `X-API-Key` header. If no API key is supplied, the client's IP address is used instead.

Each client has a token bucket with a maximum capacity of 5 tokens. Every accepted request consumes one token. Tokens are automatically replenished at a rate of 1 token per second, up to the maximum bucket capacity.

When a request arrives, the system calculates the number of tokens available based on the elapsed time since the previous update. If at least one token is available, the request is accepted and a token is consumed. Otherwise, the request is rejected with an HTTP 429 (Too Many Requests) response.

MongoDB's atomic aggregation-pipeline updates ensure that concurrent requests from the same client are processed safely. A TTL index automatically removes expired client records.

## Features

- Request tracking using API keys or IP addresses
- Token Bucket rate-limiting algorithm
- Automatic token replenishment
- Configurable bucket capacity and refill rate
- MongoDB-based persistent request tracking
- Atomic database updates for concurrent requests
- HTTP 429 responses for rate-limit violations
- Retry-After header indicating when another request may be accepted
- Rate-limit headers showing the request limit and remaining tokens
- Health-check endpoint for database connectivity

## Requirements

- Python 3
- MongoDB 4.2 or later
- FastAPI
- Uvicorn
- Motor (asynchronous MongoDB driver)
- PyMongo

## Installation

```bash
pip install -r requirements.txt
```

Start MongoDB locally or using Docker:

```bash
docker run -d -p 27017:27017 --name mongo mongo:7
```

## Execution

Start the FastAPI server:

```bash
uvicorn main:app --reload
```

The API will be available at:

`http://localhost:8000`

## Example

Send multiple requests to the API:

```bash
for i in $(seq 1 8); do
    curl -i localhost:8000/api/hello
done
```

The first five requests are accepted because the bucket initially contains 5 tokens. Subsequent requests are rejected with HTTP 429 if no tokens have been replenished.

Since tokens refill at 1 token per second, the client can make additional requests as tokens become available.

Different API keys maintain separate token buckets, allowing independent rate limits for different clients.

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/hello` | GET | Returns a response if the request is within the rate limit |
| `/health` | GET | Checks MongoDB connectivity |

## Limitations

- Requires a running MongoDB server.
- Uses a fixed default bucket capacity and refill rate, configurable through environment variables.
- Client identification relies on API keys or IP addresses.
- The implementation provides basic API rate limiting rather than a complete authentication system.
- Rate-limit records are periodically cleaned up using MongoDB's TTL mechanism.

## Conclusion

This project demonstrates the implementation of an API rate limiter using Python, FastAPI, and MongoDB. The Token Bucket algorithm allows controlled request bursts while continuously replenishing available tokens. MongoDB provides persistent storage and atomic updates, helping maintain consistent rate limits even when multiple requests arrive concurrently.
