# RAG with BORSUK

Retrieval-augmented generation over a BORSUK index: retrieve the passages nearest
to a question, and let an LLM answer from them. Three examples, simplest first —
pick the one that fits.

| Example | What it is | Needs |
|---|---|---|
| [`simple_rag.py`](simple_rag.py) | The whole idea in ~20 lines: ingest a file, then chat | `openai` + key |
| [`langchain_rag.py`](langchain_rag.py) | BORSUK as a **LangChain** vector store / retriever in an LCEL chain | `langchain`, `langchain-openai` + key |
| [`rag_chat.py`](rag_chat.py) | Batteries-included: chunking, S3, metadata filtering, and an **offline demo** with no API key | nothing to start |

## Simplest — ingest a file, then chat

```bash
export OPENAI_API_KEY=sk-...
python examples/rag/simple_rag.py notes.md
# Ingested 12 chunks. Ask a question (Ctrl-D to quit):
# > what changed in v2?
```

That is the entire pattern: embed the file's paragraphs into a BORSUK index with
the text stored as metadata, then for each question embed it, search, and answer
from the retrieved chunks.

## With LangChain (and LangGraph)

BORSUK plugs in wherever LangChain expects a `VectorStore` or retriever, so it
works in chains, agents, and LangGraph nodes unchanged:

```python
from borsuk.compat.langchain import BorsukVectorStore
from langchain_openai import OpenAIEmbeddings

store = BorsukVectorStore.from_texts(chunks, OpenAIEmbeddings(), uri="file:///tmp/rag")
retriever = store.as_retriever(search_kwargs={"k": 4})   # drop into any chain
```

Run the full chain: `python examples/rag/langchain_rag.py notes.md`. Store the
index in object storage by passing `uri="s3://bucket/prefix"`.

## No key? Run the offline demo

`rag_chat.py` runs the whole pipeline with **no account** — a built-in corpus,
toy embeddings, and an extractive answer — so you can watch retrieval work, then
upgrade to real embeddings with a key:

```bash
python examples/rag/rag_chat.py --ask "how does borsuk keep memory low?"   # offline
export OPENAI_API_KEY=sk-...
python examples/rag/rag_chat.py --docs ./my-notes                          # real, your files
export BORSUK_URI=s3://my-bucket/rag-index                                  # ...stored in S3
```

Nothing else to deploy: your bucket holds the index, the script is the client.

## How it works — the four RAG steps

The script is deliberately small so you can read the whole pipeline. Each step
maps to a function in `rag_chat.py`:

1. **Ingest** (`build_index`). Documents are split into overlapping ~500-character
   chunks. Each chunk is embedded into a vector, and the vector is added to a
   BORSUK index **with the chunk text stored as metadata**:

   ```python
   index = borsuk.create(uri=uri, metric="cosine", dimensions=backend.dimensions)
   index.add(vectors, ids=ids, metadata=[{"text": chunk, "source": path}, ...])
   ```

   Storing the text in metadata means retrieval returns the passage directly —
   there is no second document store to keep in sync with the vectors.

2. **Retrieve** (`answer_question`). The question is embedded and BORSUK returns
   the nearest chunks, metadata included:

   ```python
   report = index.search_with_report(query_vector, k=4, include_metadata=True)
   passages = [hit.metadata["text"] for hit in report.hits]
   ```

   `report` also carries `bytes_read`, so you can see exactly how little I/O a
   query costs.

3. **Augment.** The retrieved passages are concatenated into a context block.

4. **Generate.** An LLM is asked to answer using only that context, so the answer
   is grounded in your documents and can cite its sources.

## Swapping the model

`OpenAIBackend` and `LocalBackend` share one tiny interface — `embed(texts)` and
`answer(question, context)`. To use a different embedding model, local model, or
LLM, write a class with those two methods and return it from `make_backend()`.
The index dimension follows `backend.dimensions`, so nothing else changes.

## Add metadata filtering

Because every chunk is a normal BORSUK record, you can attach any metadata at
ingest (author, date, tag, tenant) and filter retrieval by it — for per-user or
per-collection RAG:

```python
report = index.search_with_report(
    query_vector, k=4, include_metadata=True,
    filter={"tenant": "acme", "year": {"$gte": 2024}},
)
```

See [`docs/api.md`](../../docs/api.md#metadata-and-filtered-search) for the full
filter reference.
