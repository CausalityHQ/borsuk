# RAG with BORSUK

A complete, one-file **retrieval-augmented generation** chatbot:
[`rag_chat.py`](rag_chat.py). It answers questions about your documents by
retrieving the most relevant passages from a BORSUK index and asking an LLM to
answer from them. Point it at a local folder or an S3 bucket; run it offline as a
demo or with OpenAI for real answers.

## Run it in 30 seconds

```bash
# from the repo root, with the borsuk package importable
python examples/rag/rag_chat.py --ask "how does borsuk keep memory low?"
```

With no API key this uses a tiny built-in corpus, **offline** toy embeddings, and
an extractive answer — enough to watch retrieval and grounding work. For real
answers, add an OpenAI key:

```bash
export OPENAI_API_KEY=sk-...
python examples/rag/rag_chat.py            # interactive chat over the built-in corpus
```

## Point it at your own documents

```bash
python examples/rag/rag_chat.py --docs ./my-notes --ask "what did we decide about billing?"
```

`--docs` ingests every `.txt` / `.md` file under a directory. Add files and rerun
to re-index.

## Store the index in S3 (or MinIO, GCS, …)

The index is just a bucket path. Set `BORSUK_URI` and the standard object-store
credentials, and the same code runs against object storage:

```bash
export AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...  AWS_REGION=us-east-1
export BORSUK_URI=s3://my-bucket/rag-index
export OPENAI_API_KEY=sk-...
python examples/rag/rag_chat.py --docs ./my-notes
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
