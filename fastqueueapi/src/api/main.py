from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Any, List, Optional
from pydantic import BaseModel
from threading import Lock


app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================
# In-memory queue and threading lock for concurrency safety
# ==============================
# Using global scope for queue and lock ensures persistence across FastAPI requests
queue: List[Any] = []
queue_lock = Lock()


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
    Add a message to the queue.
    """
    with queue_lock:
        queue.append(req.payload)
        size = len(queue)
    return EnqueueResponse(status="enqueued", queue_size=size)


# PUBLIC_INTERFACE
@app.post("/dequeue", response_model=DequeueResponse)
async def dequeue_message():
    """
    Retrieve and remove the next message from the queue.
    """
    with queue_lock:
        if len(queue) == 0:
            return DequeueResponse(status="empty", message=None, queue_size=0)
        message = queue.pop(0)
        size = len(queue)
    return DequeueResponse(status="dequeued", message=message, queue_size=size)


# PUBLIC_INTERFACE
@app.get("/status", response_model=QueueStatusResponse)
async def queue_status(list_messages: bool = False):
    """
    Return status of the queue. Optionally return all pending messages.
    """
    with queue_lock:
        size = len(queue)
        pending = list(queue) if list_messages else None
    return QueueStatusResponse(queue_size=size, pending_messages=pending)
