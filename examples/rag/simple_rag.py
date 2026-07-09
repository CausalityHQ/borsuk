#!/usr/bin/env python3
"""Minimal RAG in ~20 lines: ingest a text file, then chat about it.

    export OPENAI_API_KEY=sk-...
    python examples/rag/simple_rag.py notes.md

Ingests the file, then reads questions from stdin and answers from the retrieved
passages. Needs `openai` (`pip install openai`). For a no-dependency demo, a
LangChain integration, or S3 storage, see the other files in this folder.
"""

import sys

import borsuk
from openai import OpenAI

client = OpenAI()


def embed(texts: list[str]) -> list[list[float]]:
    data = client.embeddings.create(model="text-embedding-3-small", input=texts).data
    return [item.embedding for item in data]


# 1. Ingest: split the file into paragraphs, embed them, store the text as metadata.
chunks = [c.strip() for c in open(sys.argv[1]).read().split("\n\n") if c.strip()]
index = borsuk.create(uri="file:///tmp/simple-rag", metric="cosine", dimensions=1536)
index.add(
    embed(chunks),
    ids=[str(i) for i in range(len(chunks))],
    metadata=[{"text": c} for c in chunks],
)
print(f"Ingested {len(chunks)} chunks. Ask a question (Ctrl-D to quit):")

# 2. Chat: retrieve the nearest chunks and answer from them.
for question in sys.stdin:
    hits = index.search_with_report(
        embed([question])[0], k=4, include_metadata=True
    ).hits
    context = "\n\n".join(hit.metadata["text"] for hit in hits)
    reply = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "user",
                "content": f"Answer only from this context:\n{context}\n\nQuestion: {question}",
            }
        ],
    )
    print("\n" + reply.choices[0].message.content + "\n")
