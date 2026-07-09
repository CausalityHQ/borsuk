#!/usr/bin/env python3
"""RAG with LangChain + BORSUK: BORSUK is the vector store / retriever.

    pip install langchain langchain-openai
    export OPENAI_API_KEY=sk-...
    python examples/rag/langchain_rag.py notes.md

BORSUK plugs in wherever LangChain (or LangGraph) expects a `VectorStore` or
retriever, so you get object-storage-backed retrieval with the LangChain
ecosystem — chains, agents, LangGraph nodes — unchanged.
"""

import sys

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

from borsuk.compat.langchain import BorsukVectorStore

# 1. Ingest the file into a BORSUK-backed LangChain vector store.
#    Swap uri= for s3://bucket/prefix to store the index in object storage.
chunks = [c.strip() for c in open(sys.argv[1]).read().split("\n\n") if c.strip()]
store = BorsukVectorStore.from_texts(chunks, OpenAIEmbeddings(), uri="file:///tmp/langchain-rag")
retriever = store.as_retriever(search_kwargs={"k": 4})

# 2. A standard LCEL RAG chain — the retriever is the only BORSUK-specific part.
prompt = ChatPromptTemplate.from_template(
    "Answer the question using only the context.\n\nContext:\n{context}\n\nQuestion: {question}"
)
chain = (
    {"context": retriever, "question": RunnablePassthrough()}
    | prompt
    | ChatOpenAI(model="gpt-4o-mini")
    | StrOutputParser()
)

print("Ask a question (Ctrl-D to quit):")
for question in sys.stdin:
    if question.strip():
        print("\n" + chain.invoke(question.strip()) + "\n")
