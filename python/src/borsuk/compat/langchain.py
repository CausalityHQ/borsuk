"""LangChain vector store backed by BORSUK.

Use BORSUK anywhere a LangChain (or LangGraph) ``VectorStore`` / retriever is
expected — object-storage-backed retrieval with near-zero resident memory::

    from borsuk.compat.langchain import BorsukVectorStore
    from langchain_openai import OpenAIEmbeddings

    store = BorsukVectorStore.from_texts(
        ["the cat sat", "the dog ran"],
        OpenAIEmbeddings(),
        uri="file:///tmp/rag-index",          # or s3://bucket/prefix
        metadatas=[{"src": "a"}, {"src": "b"}],
    )
    retriever = store.as_retriever(search_kwargs={"k": 3})
    docs = retriever.invoke("what did the cat do?")

Requires ``langchain-core`` (installed with ``langchain``). The document text is
stored in each vector's metadata, so retrieval returns the passage directly — no
separate document store to keep in sync.
"""

from __future__ import annotations

from typing import Any, Iterable

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

import borsuk

# Reserved metadata keys so page content and score survive the round trip
# without colliding with user metadata.
_TEXT_KEY = "__page_content__"

_METRIC_ALIASES = {"l2": "euclidean", "cosine": "cosine", "ip": "inner-product"}


class BorsukVectorStore(VectorStore):
    """A LangChain ``VectorStore`` whose index lives in a BORSUK bucket."""

    def __init__(self, index: Any, embedding: Embeddings) -> None:
        self._index = index
        self._embedding = embedding

    @property
    def embeddings(self) -> Embeddings:
        return self._embedding

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: list[dict] | None = None,
        *,
        ids: list[str] | None = None,
        **_: Any,
    ) -> list[str]:
        texts = list(texts)
        if not texts:
            return []
        vectors = self._embedding.embed_documents(texts)
        ids = ids or [str(i) for i in range(self._count(), self._count() + len(texts))]
        metadata = []
        for position, text in enumerate(texts):
            entry = dict(metadatas[position]) if metadatas else {}
            entry[_TEXT_KEY] = text
            metadata.append(entry)
        # Overwrite any existing ids (LangChain upsert semantics).
        present = [i for i in ids if self._index.get_record(i) is not None]
        if present:
            self._index.delete(present)
            self._index.purge()
        self._index.add(vectors, ids=[str(i) for i in ids], metadata=metadata)
        return [str(i) for i in ids]

    def similarity_search(
        self, query: str, k: int = 4, filter: dict | None = None, **_: Any
    ) -> list[Document]:
        return [doc for doc, _score in self.similarity_search_with_score(query, k, filter)]

    def similarity_search_with_score(
        self, query: str, k: int = 4, filter: dict | None = None, **_: Any
    ) -> list[tuple[Document, float]]:
        query_vector = self._embedding.embed_query(query)
        report = self._index.search_with_report(
            query_vector, k=k, filter=filter, include_metadata=True
        )
        results = []
        for hit in report.hits:
            metadata = dict(hit.metadata or {})
            text = metadata.pop(_TEXT_KEY, "")
            results.append((Document(page_content=text, metadata=metadata), hit.distance))
        return results

    def delete(self, ids: list[str] | None = None, **_: Any) -> bool | None:
        if not ids:
            return False
        self._index.delete([str(i) for i in ids])
        self._index.purge()
        return True

    @classmethod
    def from_texts(
        cls,
        texts: list[str],
        embedding: Embeddings,
        metadatas: list[dict] | None = None,
        *,
        uri: str = "file:///tmp/borsuk-langchain-index",
        metric: str = "cosine",
        ids: list[str] | None = None,
        **_: Any,
    ) -> BorsukVectorStore:
        dimensions = len(embedding.embed_query(texts[0] if texts else "probe"))
        index = borsuk.create(
            uri=uri, metric=_METRIC_ALIASES.get(metric, metric), dimensions=dimensions
        )
        store = cls(index, embedding)
        if texts:
            store.add_texts(texts, metadatas=metadatas, ids=ids)
        return store

    def _count(self) -> int:
        return self._index.stats().records
