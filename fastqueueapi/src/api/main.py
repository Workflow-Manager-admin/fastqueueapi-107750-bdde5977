from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Any, List, Optional
from pydantic import BaseModel
import os
import redis
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================
# Redis-backed queue for cross-process safety and durability
# ==============================
# The queue is managed using a Redis list, so all FastAPI processes/instances see the same queue state.
# Redis settings: can be configured via environment variables.

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_QUEUE_DEFAULT = "fastqueueapi_queue"
REDIS_QUEUE_NAME = os.environ.get(
    "REDIS_QUEUE_NAME",
    REDIS_QUEUE_DEFAULT
)

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)

# ==============================
# Request/Response Models
# ==============================


class EnqueueRequest(BaseModel):
    """Model for the message to be enqueued. Accepts arbitrary payload."""
    payload: Any


class EnqueueResponse(BaseModel):
    """Response after enqueuing a message."""
    status: str
    queue_size: int


class DequeueResponse(BaseModel):
    """Response after dequeuing a message."""
    status: str
    message: Optional[Any] = None
    queue_size: int


class QueueStatusResponse(BaseModel):
    """Status of the queue."""
    queue_size: int
    pending_messages: Optional[List[Any]] = None


# PUBLIC_INTERFACE
@app.get("/")
def health_check():
    """Quick health check endpoint."""
    return {"message": "Healthy"}


# PUBLIC_INTERFACE
@app.post("/enqueue", response_model=EnqueueResponse)
async def enqueue_message(req: EnqueueRequest):
    """
    Add a message to the queue using Redis (LPUSH for queue semantics).
    LPUSH (left-push) adds to head; we'll use RPUSH for FIFO (right-push), so we dequeue from left.
    """
    # Serialize the payload as JSON to store in Redis list
    serialized = json.dumps(req.payload)
    redis_client.rpush(REDIS_QUEUE_NAME, serialized)
    size = redis_client.llen(REDIS_QUEUE_NAME)
    return EnqueueResponse(status="enqueued", queue_size=size)


# PUBLIC_INTERFACE
@app.post("/dequeue", response_model=DequeueResponse)
async def dequeue_message():
    """
    Retrieve and remove the next message from the queue.
    Uses Redis LPOP for FIFO order.
    """
    serialized = redis_client.lpop(REDIS_QUEUE_NAME)
    if serialized is None:
        size = redis_client.llen(REDIS_QUEUE_NAME)
        return DequeueResponse(status="empty", message=None, queue_size=size)
    try:
        message = json.loads(serialized)
    except Exception:
        message = serialized
    size = redis_client.llen(REDIS_QUEUE_NAME)
    return DequeueResponse(status="dequeued", message=message, queue_size=size)


# PUBLIC_INTERFACE
@app.get("/status", response_model=QueueStatusResponse)
async def queue_status(list_messages: bool = False):
    """
    Return status of the queue. Optionally return all pending messages.
    """
    size = redis_client.llen(REDIS_QUEUE_NAME)
    if list_messages:
        # Get all pending messages
        serialized_pending = redis_client.lrange(REDIS_QUEUE_NAME, 0, -1)
        try:
            pending = [json.loads(item) for item in serialized_pending]
        except Exception:
            pending = serialized_pending
    else:
        pending = None
    return QueueStatusResponse(queue_size=size, pending_messages=pending)
