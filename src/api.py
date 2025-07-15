from flask import Flask, request, jsonify
import chromadb
from sentence_transformers import SentenceTransformer
import ollama

from config.config import DB_COLLECTION_NAME, CHROMA_HOST, CHROMA_PORT, EMBEDDING_MODEL, LLM_MODEL, OLLAMA_HOST, OLLAMA_PORT

app = Flask(__name__)

@app.route('/ask', methods=['GET'])
def ask_question_endpoint():
    """
    Flask endpoint to ask a question and get an answer from the RAG pipeline.
    Takes a 'question' as a query parameter.
    """
    question = request.args.get('question')

    print(question)
    if not question:
        return jsonify({"error": "Query parameter 'question' is required."}), 400

    try:

        embedding_model = SentenceTransformer(EMBEDDING_MODEL)
        chroma_client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
        collection = chroma_client.get_collection(name=DB_COLLECTION_NAME)
        ollama_client = ollama.Client(host=f"http://{OLLAMA_HOST}:{OLLAMA_PORT}")
        print(f"http://{OLLAMA_HOST}:{OLLAMA_PORT}")
        print("Initialization complete.")
        
        print(f"\nEmbedding the query: '{question}'")
        query_embedding = embedding_model.encode(question).tolist()
        
        client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
        collection = client.get_collection(name=DB_COLLECTION_NAME)

        print("Retrieving relevant documents from ChromaDB...")
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=3,
            include=["documents", "metadatas","distances"]
        )
        
        context = "\n\n---\n\n".join(results['documents'][0])
        sources = results['metadatas'][0]
        print("Retrieval complete.")

        # --- Generation ---
        if not context:
             answer = "I do not have enough information to answer this question."
             source = "No source found"
        else:
            print("Generating answer with the LLM...")
            prompt = f"""
            You are a helpful and concise bookstore assistant. Your task is to answer the user's question based *only* on the provided context.
            Follow these rules strictly:
            1. Directly answer the user's question using information found in the context.
            2. Do not use any outside knowledge or make up information.
            3. If the context does not contain the information needed to answer the question, you must respond with the exact phrase: "I do not have enough information to answer this question."

            Here is the context:
            ---
            {context}
            ---

            Question: {question}

            Answer:
            """
            response = ollama_client.chat(
                model=LLM_MODEL,
                messages=[{'role': 'user', 'content': prompt}]
            )
            answer = response['message']['content']
            print("Generation complete.")
            
            # Extract the source URL from the most relevant document
            source = sources[0].get('book_source_url', 'Source URL not available') if 'I do not have enough information' not in answer else "No source found"

        return jsonify({"answer": answer, "source": source})

    except Exception as e:
        print(f"An error occurred during RAG query: {e}")
        return jsonify({"error": "An internal error occurred."}), 500