"""
HTTP server — ops agent only.

POST /query   {"question": "..."}  → {"answer": "..."}
GET  /health                       → {"status": "ok"}

Primary runtime is Kafka consumer mode (run_consumer() in graph.py).
This server is for ops tooling: ask questions about the ML platform,
trigger Katib HPO, check model latency, etc.

Start:
    uvicorn agent.server:server --host 0.0.0.0 --port 8000
"""

from fastapi import FastAPI, HTTPException
from langchain_core.messages import HumanMessage
from pydantic import BaseModel

from agent.graph import ops_app

server = FastAPI(title="Anomaly Detector — Ops Agent")


# ── /health ───────────────────────────────────────────────

@server.get("/health")
def health():
    return {"status": "ok"}


# ── /query — ops ReAct agent ──────────────────────────────

class Query(BaseModel):
    question: str


class Answer(BaseModel):
    answer: str


@server.post("/query", response_model=Answer)
async def query(q: Query):
    try:
        result = ops_app.invoke({"messages": [HumanMessage(content=q.question)]})
        last = result["messages"][-1]
        return {"answer": last.content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
