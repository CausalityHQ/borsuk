#!/usr/bin/env python3
"""End-to-end RAG (retrieval-augmented generation) over a BORSUK index.

One file, four steps:

  1. INGEST  — read documents, split them into chunks, embed each chunk, and
               store the vectors + the chunk text (as metadata) in a BORSUK index.
  2. RETRIEVE — embed the user's question and ask BORSUK for the nearest chunks.
  3. AUGMENT  — stitch those chunks into a prompt as grounding context.
  4. GENERATE — ask an LLM to answer using only that context.

The index is just a bucket path: `file://…` for local, `s3://bucket/prefix` for
object storage. Embeddings and the chat model come from OpenAI when
`OPENAI_API_KEY` is set; otherwise a tiny built-in fallback runs the whole demo
offline (toy embeddings + an extractive answer) so you can see retrieval work
without any account.

    # Offline demo over the built-in BORSUK corpus:
    python examples/rag/rag_chat.py

    # Real RAG with OpenAI over your own documents, stored in S3:
    export OPENAI_API_KEY=sk-...
    export BORSUK_URI=s3://my-bucket/rag-index      # optional; defaults to a temp dir
    python examples/rag/rag_chat.py --docs ./my-notes --ask "what changed in v2?"

Requires `borsuk` (this repo) and, for real embeddings, `openai` (`pip install openai`).
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

import borsuk

# ---------------------------------------------------------------------------
# Embedding + generation: OpenAI when configured, a deterministic local
# fallback otherwise. Both expose the same tiny interface so the RAG pipeline
# below never cares which one it got.
# ---------------------------------------------------------------------------


class OpenAIBackend:
    """Real embeddings + chat via OpenAI. Used when OPENAI_API_KEY is set."""

    dimensions = 1536

    def __init__(self) -> None:
        from openai import OpenAI  # imported lazily so the offline demo needs no dep

        self._client = OpenAI()
        self._embed_model = os.environ.get(
            "BORSUK_EMBED_MODEL", "text-embedding-3-small"
        )
        self._chat_model = os.environ.get("BORSUK_CHAT_MODEL", "gpt-4o-mini")

    def embed(self, texts: list[str]) -> list[list[float]]:
        response = self._client.embeddings.create(model=self._embed_model, input=texts)
        return [item.embedding for item in response.data]

    def answer(self, question: str, context: str) -> str:
        response = self._client.chat.completions.create(
            model=self._chat_model,
            messages=[
                {
                    "role": "system",
                    "content": "Answer using only the provided context. If it is not in the context, say you don't know.",
                },
                {
                    "role": "user",
                    "content": f"Context:\n{context}\n\nQuestion: {question}",
                },
            ],
        )
        return response.choices[0].message.content or ""


class LocalBackend:
    """Deterministic hash-based embeddings + an extractive answer. No account,
    low quality — enough to watch retrieval and grounding work offline."""

    dimensions = 256

    def embed(self, texts: list[str]) -> list[list[float]]:
        import hashlib
        import math

        vectors = []
        for text in texts:
            vector = [0.0] * self.dimensions
            for token in text.lower().split():
                digest = hashlib.blake2b(token.encode(), digest_size=8).digest()
                bucket = int.from_bytes(digest[:4], "big") % self.dimensions
                sign = 1.0 if digest[4] & 1 else -1.0
                vector[bucket] += sign
            norm = math.sqrt(sum(component * component for component in vector)) or 1.0
            vectors.append([component / norm for component in vector])
        return vectors

    def answer(self, question: str, context: str) -> str:
        return (
            "[offline demo — set OPENAI_API_KEY for a real answer]\n"
            "Most relevant retrieved context:\n" + context.split("\n\n")[0]
        )


def make_backend() -> OpenAIBackend | LocalBackend:
    if os.environ.get("OPENAI_API_KEY"):
        try:
            return OpenAIBackend()
        except Exception as exc:  # missing package, bad key, etc.
            print(
                f"! OpenAI unavailable ({exc}); using the offline fallback.",
                file=sys.stderr,
            )
    else:
        print(
            "! OPENAI_API_KEY not set; using the offline demo backend.", file=sys.stderr
        )
    return LocalBackend()


# ---------------------------------------------------------------------------
# Documents -> chunks
# ---------------------------------------------------------------------------


def chunk(text: str, size: int = 500, overlap: int = 80) -> list[str]:
    """Split text into overlapping character windows on paragraph boundaries."""
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks: list[str] = []
    buffer = ""
    for paragraph in paragraphs:
        if len(buffer) + len(paragraph) <= size:
            buffer = f"{buffer}\n\n{paragraph}".strip()
        else:
            if buffer:
                chunks.append(buffer)
            buffer = (
                (buffer[-overlap:] + "\n\n" + paragraph).strip()
                if buffer
                else paragraph
            )
            while len(buffer) > size:
                chunks.append(buffer[:size])
                buffer = buffer[size - overlap :]
    if buffer:
        chunks.append(buffer)
    return chunks


BUILTIN_CORPUS = {
    "borsuk-overview.md": (
        "BORSUK is a vector-search library that keeps the whole index as immutable "
        "Parquet objects in object storage such as S3, MinIO, or GCS.\n\n"
        "It answers a query with only a few hundred bytes of resident memory, because "
        "routing is paged: the query walks a small tree of centroids to the handful of "
        "segments that could hold a neighbour, and reads only those objects."
    ),
    "borsuk-filtering.md": (
        "Every vector in BORSUK can carry schemaless metadata, and any search can be "
        "constrained by a Pinecone-style filter such as {'genre': 'rock'}.\n\n"
        "Filtering happens before ranking, so a selective filter is exact and skips "
        "whole segments that cannot match, reading far fewer objects."
    ),
    "borsuk-adapters.md": (
        "BORSUK ships drop-in adapters for Pinecone, turbopuffer, Amazon S3 Vectors, "
        "Chroma, and Qdrant.\n\n"
        "You change the import and point it at a bucket; your upsert, query, and filter "
        "calls stay the same, so existing code can switch backends without a rewrite."
    ),
}


def load_documents(docs_dir: str | None) -> list[tuple[str, str]]:
    """Return (source, text) pairs — from a directory of .txt/.md files, or the
    built-in BORSUK corpus when no directory is given."""
    if docs_dir is None:
        return list(BUILTIN_CORPUS.items())
    documents = []
    for path in sorted(Path(docs_dir).rglob("*")):
        if path.suffix.lower() in {".txt", ".md"} and path.is_file():
            documents.append(
                (str(path), path.read_text(encoding="utf-8", errors="ignore"))
            )
    if not documents:
        raise SystemExit(f"no .txt/.md documents found under {docs_dir}")
    return documents


# ---------------------------------------------------------------------------
# The pipeline
# ---------------------------------------------------------------------------


def build_index(
    uri: str, backend: OpenAIBackend | LocalBackend, documents: list[tuple[str, str]]
):
    """Step 1 — INGEST: chunk, embed, and store text as metadata in BORSUK."""
    index = borsuk.create(uri=uri, metric="cosine", dimensions=backend.dimensions)
    ids: list[str] = []
    vectors: list[list[float]] = []
    metadata: list[dict] = []
    for source, text in documents:
        for position, piece in enumerate(chunk(text)):
            ids.append(f"{source}#{position}")
            metadata.append({"text": piece, "source": source})
    # Embed in one batch, then add. `metadata` holds the chunk text so retrieval
    # returns the passage directly — no separate document store to keep in sync.
    vectors = backend.embed([m["text"] for m in metadata])
    index.add(vectors, ids=ids, metadata=metadata)
    print(f"Ingested {len(ids)} chunks from {len(documents)} document(s) into {uri}")
    return index


def answer_question(
    index, backend: OpenAIBackend | LocalBackend, question: str, k: int = 4
) -> str:
    """Steps 2-4 — RETRIEVE nearest chunks, AUGMENT a prompt, GENERATE an answer."""
    query_vector = backend.embed([question])[0]
    report = index.search_with_report(query_vector, k=k, include_metadata=True)
    passages = [hit.metadata["text"] for hit in report.hits]
    sources = [hit.metadata["source"] for hit in report.hits]
    context = "\n\n".join(passages)
    answer = backend.answer(question, context)
    citations = ", ".join(dict.fromkeys(sources))  # unique, order-preserving
    return f"{answer}\n\n— retrieved from: {citations} ({report.bytes_read} bytes read)"


def main() -> None:
    parser = argparse.ArgumentParser(description="RAG chat over a BORSUK index.")
    parser.add_argument(
        "--docs", help="directory of .txt/.md files (default: built-in corpus)"
    )
    parser.add_argument(
        "--uri",
        default=os.environ.get("BORSUK_URI"),
        help="index URI (file:// or s3://)",
    )
    parser.add_argument(
        "--ask", help="ask one question and exit (default: interactive chat)"
    )
    args = parser.parse_args()

    backend = make_backend()
    documents = load_documents(args.docs)

    # Default to a throwaway local index so the demo runs with zero setup.
    tmp = None
    uri = args.uri
    if uri is None:
        tmp = tempfile.mkdtemp(prefix="borsuk-rag-")
        uri = Path(tmp).as_uri()

    index = build_index(uri, backend, documents)

    if args.ask:
        print("\n" + answer_question(index, backend, args.ask))
        return

    print("\nAsk a question (blank line or Ctrl-D to quit):")
    try:
        while True:
            question = input("\n> ").strip()
            if not question:
                break
            print("\n" + answer_question(index, backend, question))
    except (EOFError, KeyboardInterrupt):
        pass


if __name__ == "__main__":
    main()
