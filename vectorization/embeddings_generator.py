"""
Embeddings Generator
Generates vector embeddings for text chunks using SentenceTransformers
"""

from sentence_transformers import SentenceTransformer
from typing import List, Dict
import numpy as np
from tqdm import tqdm
try:
    from .config import config
except ImportError:
    from config import config

class EmbeddingsGenerator:
    """
    Generate embeddings for text chunks
    """
    
    def __init__(self, model_name: str = None):
        """
        Initialize embeddings generator
        
        Args:
            model_name: SentenceTransformer model name (defaults to config)
        """
        
        self.model_name = model_name or config.EMBEDDING_MODEL
        
        print(f"Loading embedding model: {self.model_name}...")
        self.model = SentenceTransformer(self.model_name)
        
        self.embedding_dim = self.model.get_sentence_embedding_dimension()
        
        print(f"   Model loaded")
        print(f"   Embedding dimension: {self.embedding_dim}")
        print(f"   Max sequence length: {self.model.max_seq_length}")
    
    def generate_single(self, text: str) -> List[float]:
        """
        Generate embedding for single text
        
        Args:
            text: Input text
        
        Returns:
            Embedding vector
        """
        
        embedding = self.model.encode(text, convert_to_numpy=True)
        return embedding.tolist()
    
    def generate_batch(
        self,
        texts: List[str],
        batch_size: int = None,
        show_progress: bool = True
    ) -> List[List[float]]:
        """
        Generate embeddings for batch of texts
        
        Args:
            texts: List of text strings
            batch_size: Batch size for encoding (defaults to config)
            show_progress: Show progress bar
        
        Returns:
            List of embedding vectors
        """
        
        batch_size = batch_size or config.BATCH_SIZE
        
        embeddings = self.model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=show_progress,
            convert_to_numpy=True
        )
        
        return embeddings.tolist()
    
    def generate_for_chunks(
        self,
        chunks: List[Dict],
        batch_size: int = None,
        show_progress: bool = True
    ) -> List[Dict]:
        """
        Generate embeddings for document chunks
        
        Args:
            chunks: List of chunk dictionaries (must have 'text' field)
            batch_size: Batch size
            show_progress: Show progress bar
        
        Returns:
            Chunks with 'embedding' field added
        """
        
        # Extract texts
        texts = [chunk['text'] for chunk in chunks]
        
        # Generate embeddings
        print(f"Generating embeddings for {len(texts)} chunks...")
        embeddings = self.generate_batch(
            texts,
            batch_size=batch_size,
            show_progress=show_progress
        )
        
        # Add embeddings to chunks
        for chunk, embedding in zip(chunks, embeddings):
            chunk['embedding'] = embedding
        
        print(f" Generated {len(embeddings)} embeddings")
        
        return chunks
    
    def validate_embedding(self, embedding: List[float]) -> bool:
        """
        Validate embedding vector
        
        Args:
            embedding: Embedding vector
        
        Returns:
            True if valid
        """
        
        # Check dimension
        if len(embedding) != self.embedding_dim:
            return False
        
        # Check for NaN or Inf
        if not all(np.isfinite(embedding)):
            return False
        
        # Check magnitude (should not be zero vector)
        magnitude = np.linalg.norm(embedding)
        if magnitude < 1e-6:
            return False
        
        return True
    
    def get_similarity(
        self,
        embedding1: List[float],
        embedding2: List[float]
    ) -> float:
        """
        Calculate cosine similarity between two embeddings
        
        Args:
            embedding1: First embedding
            embedding2: Second embedding
        
        Returns:
            Cosine similarity (0-1)
        """
        
        vec1 = np.array(embedding1)
        vec2 = np.array(embedding2)
        
        # Cosine similarity
        similarity = np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
        
        return float(similarity)


# Example usage
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING EMBEDDINGS GENERATOR")
    print("="*70)
    
    # Initialize generator
    generator = EmbeddingsGenerator()
    
    # Test single embedding
    print("\n[TEST 1] Single Embedding:")
    text = "The chiller startup procedure requires oil heater warm-up for 12 hours."
    embedding = generator.generate_single(text)
    
    print(f"  Text: {text}")
    print(f"  Embedding dimension: {len(embedding)}")
    print(f"  First 5 values: {embedding[:5]}")
    print(f"  Valid: {generator.validate_embedding(embedding)}")
    
    # Test batch embeddings
    print("\n[TEST 2] Batch Embeddings:")
    texts = [
        "Chiller startup requires pre-checks",
        "Verify oil temperature before starting",
        "Monitor discharge pressure during startup",
        "CHW supply should reach 6.5°C within 5 minutes"
    ]
    
    embeddings = generator.generate_batch(texts, show_progress=True)
    print(f"  Generated {len(embeddings)} embeddings")
    
    # Test similarity
    print("\n[TEST 3] Similarity Calculation:")
    sim_1_2 = generator.get_similarity(embeddings[0], embeddings[1])
    sim_1_3 = generator.get_similarity(embeddings[0], embeddings[2])
    
    print(f"  Similarity (text 1 vs 2): {sim_1_2:.4f}")
    print(f"  Similarity (text 1 vs 3): {sim_1_3:.4f}")
    
    # Test with chunks
    print("\n[TEST 4] Embeddings for Chunks:")
    chunks = [
        {
            'text': texts[0],
            'source': 'test.pdf',
            'page': 1
        },
        {
            'text': texts[1],
            'source': 'test.pdf',
            'page': 1
        }
    ]
    
    chunks_with_embeddings = generator.generate_for_chunks(chunks, show_progress=False)
    print(f"  Chunks processed: {len(chunks_with_embeddings)}")
    print(f"  First chunk has embedding: {'embedding' in chunks_with_embeddings[0]}")
    
    print("\n All tests passed!")