"""
RAG retrieval — pgvector k-NN over infra changes since last_train_ts.

embed() calls Bedrock Titan Text Embeddings v2 (1536-dim, normalized).
retrieve_infra_context() queries rag_chunks using cosine similarity (<=>).
"""

import json
import os
from typing import Optional

import boto3

from agent.db import get_db

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
_EMBED_MODEL = "amazon.titan-embed-text-v2:0"


def embed(text: str) -> list[float]:
    """
    Embed text with Bedrock Titan Text Embeddings v2.
    Returns 1536-dimensional normalized vector (cosine via inner product).
    """
    bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    resp = bedrock.invoke_model(
        modelId=_EMBED_MODEL,
        body=json.dumps({"inputText": text, "dimensions": 1536, "normalize": True}),
    )
    return json.loads(resp["body"].read())["embedding"]


def retrieve_infra_context(
    event: dict,
    last_train_ts: str,
    k: int = 5,
) -> list[dict]:
    """
    Find the k infra-change CloudTrail events most semantically similar to
    the flagged anomaly event, scoped to changes since last_train_ts.

    Returns list of dicts with keys: raw_text, event_name, event_time,
    resource, principal.  Returns [] if rag_chunks is empty or embed fails.
    """
    query = " ".join(filter(None, [
        event.get("eventName", ""),
        event.get("eventSource", ""),
        event.get("errorCode", ""),
    ]))

    try:
        query_vec = embed(query)
    except Exception:
        return []   # graceful degradation — LLM will classify with no RAG context

    conn = get_db()
    try:
        with conn.cursor() as cur:
            # pgvector <=> = cosine distance; ORDER BY ASC = most similar first
            cur.execute(
                """
                SELECT raw_text, event_name, event_time::text, resource, principal
                FROM   rag_chunks
                WHERE  event_time > %s::timestamptz
                ORDER  BY embedding <=> %s::vector
                LIMIT  %s
                """,
                (last_train_ts, json.dumps(query_vec), k),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()
