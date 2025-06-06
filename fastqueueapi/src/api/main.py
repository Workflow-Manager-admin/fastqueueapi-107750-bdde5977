import os
import json
import fcntl
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Any, List, Optional
from pydantic import BaseModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================
# File-backed queue for cross-process safety and durability
# ==============================
QUEUE_FILE_PATH = os.environ.get("FASTQUEUE_FILE", "./fastqueue_file.queue")  # Default file path


def _acquire_lock(fd):
    """
    Acquire an exclusive lock on the given file descriptor.
    """
    fcntl.flock(fd, fcntl.LOCK_EX)


def _release_lock(fd):
    """
    Release the lock on the given file descriptor.
    """
    fcntl.flock(fd, fcntl.LOCK_UN)


def _read_queue():
    """
    Safely read all items from the queue file. Returns a list.
    """
    if not os.path.exists(QUEUE_FILE_PATH):
        return []
    with open(QUEUE_FILE_PATH, "r") as f:
        _acquire_lock(f)
        try:
            f.seek(0)
            lines = f.readlines()
            messages = []
            for line in lines:
                line = line.strip()
                if line:
                    try:
                        messages.append(json.loads(line))
                    except Exception:
                        messages.append(line)
            return messages
        finally:
            _release_lock(f)


def _write_queue(messages):
    """
    Overwrite the queue file atomically with the given list.
    """
    with open(QUEUE_FILE_PATH, "w") as f:
        _acquire_lock(f)
        try:
            for msg in messages:
                if not isinstance(msg, str):
                    f.write(json.dumps(msg) + "\n")
                else:
                    f.write(msg.rstrip("\n") + "\n")
            f.flush()
            os.fsync(f.fileno())
        finally:
            _release_lock(f)


def _append_to_queue(msg):
    """
    Appends a message to the queue file (FIFO).
    """
    with open(QUEUE_FILE_PATH, "a") as f:
        _acquire_lock(f)
        try:
            if not isinstance(msg, str):
                msg = json.dumps(msg)
            f.write(msg + "\n")
            f.flush()
            os.fsync(f.fileno())
        finally:
            _release_lock(f)


def _pop_from_queue():
    """
    Pops (removes and returns) the first message from the queue file. Returns (msg, queue_size).
    """
    # Read and lock the whole queue, remove first item, write back
    if not os.path.exists(QUEUE_FILE_PATH):
        return None, 0
    with open(QUEUE_FILE_PATH, "r+") as f:
        _acquire_lock(f)
        try:
            f.seek(0)
            lines = f.readlines()
            if not lines:
                return None, 0
            first_line = lines[0].strip()
            rest = lines[1:]
            f.seek(0)
            f.truncate(0)
            for line in rest:
                f.write(line)
            f.flush()
            os.fsync(f.fileno())
            try:
                first_msg = json.loads(first_line)
            except Exception:
                first_msg = first_line
            return first_msg, len(rest)
        finally:
            _release_lock(f)


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
    Add a message to the queue using file-backed persistence.
    """
    _append_to_queue(req.payload)
    # Re-read size to ensure durable acknowledgement
    queue_size = len(_read_queue())
    return EnqueueResponse(status="enqueued", queue_size=queue_size)


# PUBLIC_INTERFACE
@app.post("/dequeue", response_model=DequeueResponse)
async def dequeue_message():
    """
    Retrieve and remove the next message from the queue.
    """
    message, size = _pop_from_queue()
    status = "dequeued" if message is not None else "empty"
    return DequeueResponse(status=status, message=message, queue_size=size)


# PUBLIC_INTERFACE
@app.get("/status", response_model=QueueStatusResponse)
async def queue_status(list_messages: bool = False):
    """
    Return status of the queue. Optionally return all pending messages.
    """
    messages = _read_queue()
    size = len(messages)
    pending = messages if list_messages else None
    return QueueStatusResponse(queue_size=size, pending_messages=pending)
