"""
Chunking Strategies
Different strategies for splitting documents into chunks
"""

import tiktoken
from typing import List, Dict
from abc import ABC, abstractmethod

class ChunkingStrategy(ABC):
    """Base class for chunking strategies"""
    
    @abstractmethod
    def chunk(self, text: str, metadata: Dict = None) -> List[Dict]:
        """Split text into chunks"""
        pass


class TokenBasedChunking(ChunkingStrategy):
    """
    Token-based chunking with overlap
    Best for general documents
    """
    
    def __init__(
        self,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        encoding: str = "cl100k_base"
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.tokenizer = tiktoken.get_encoding(encoding)
    
    def chunk(self, text: str, metadata: Dict = None) -> List[Dict]:
        """
        Split text into overlapping token-based chunks
        """
        
        # Tokenize
        tokens = self.tokenizer.encode(text) 
        chunks = []
        start = 0
        chunk_id = 0
        
        while start < len(tokens):
            # Get chunk
            end = min(start + self.chunk_size, len(tokens))
            chunk_tokens = tokens[start:end]
            
            # Decode to text
            chunk_text = self.tokenizer.decode(chunk_tokens)
            
            # Create chunk
            chunk = {
                'chunk_id': chunk_id,
                'text': chunk_text,
                'tokens': len(chunk_tokens),
                'start_token': start,
                'end_token': end,
                'total_tokens': len(tokens)
            }
            
            # Add metadata
            if metadata:
                chunk.update(metadata)
            
            chunks.append(chunk)
            
            # Move to next chunk
            start = end - self.chunk_overlap
            chunk_id += 1
            
            # Break if at end
            if end >= len(tokens):
                break
        
        return chunks


class SemanticChunking(ChunkingStrategy):
    """
    Semantic chunking based on sections/paragraphs
    Best for structured documents (SOPs, manuals)
    """
    
    def __init__(self, max_chunk_size: int = 1500):
        self.max_chunk_size = max_chunk_size
        self.tokenizer = tiktoken.get_encoding("cl100k_base")
    
    def chunk(self, text: str, metadata: Dict = None) -> List[Dict]:
        """
        Split text by sections/paragraphs
        """
        
        chunks = []
        chunk_id = 0
        
        # Split by double newlines (paragraphs)
        sections = text.split('\n\n')
        
        current_chunk = ""
        
        for section in sections:
            section = section.strip()
            if not section:
                continue
            
            # Check if adding this section exceeds max size
            test_chunk = current_chunk + "\n\n" + section if current_chunk else section  # where the semantic chunking happens 
            tokens = self.tokenizer.encode(test_chunk)
            
            if len(tokens) > self.max_chunk_size:
                # Save current chunk
                if current_chunk:
                    chunks.append({
                        'chunk_id': chunk_id,
                        'text': current_chunk,
                        'tokens': len(self.tokenizer.encode(current_chunk)),
                        'chunking_method': 'semantic'
                    })
                    chunk_id += 1
                
                # Start new chunk
                current_chunk = section
            else:
                # Add to current chunk
                current_chunk = test_chunk
        
        # Add final chunk
        if current_chunk:
            chunks.append({
                'chunk_id': chunk_id,
                'text': current_chunk,
                'tokens': len(self.tokenizer.encode(current_chunk)),
                'chunking_method': 'semantic'
            })
        
        # Add metadata
        if metadata:
            for chunk in chunks:
                chunk.update(metadata)
        
        return chunks


class HybridChunking(ChunkingStrategy):
    """
    Hybrid: Try semantic first, fall back to token-based
    Best overall strategy
    """
    
    def __init__(
        self,
        semantic_max: int = 1500,
        token_size: int = 1000,
        token_overlap: int = 200
    ):
        self.semantic = SemanticChunking(max_chunk_size=semantic_max)
        self.token_based = TokenBasedChunking(
            chunk_size=token_size,
            chunk_overlap=token_overlap
        )
    
    def chunk(self, text: str, metadata: Dict = None) -> List[Dict]:
        """
        Try semantic chunking first, use token-based as fallback
        """
        
        # Try semantic
        semantic_chunks = self.semantic.chunk(text, metadata)
        
        # If semantic produces good chunks, use them
        if len(semantic_chunks) >= 2:
            return semantic_chunks
        
        # Otherwise fall back to token-based
        return self.token_based.chunk(text, metadata)


# Example usage
if __name__ == "__main__":
    
    sample_text = """
    STANDARD OPERATING PROCEDURE
    SOP-CP-001: Chiller Startup
    
    1. PRE-STARTUP CHECKS
    
    1.1 Oil Heater Warm-up
    Verify oil heater has been energized for minimum 12 hours.
    Check oil temperature is at least 50°C before starting.
    
    1.2 System Checks
    Verify CHW and CW valves are open.
    Confirm proper water flow through evaporator and condenser.
    
    2. STARTUP SEQUENCE
    
    2.1 Initiate Startup
    Press START button on chiller control panel.
    Monitor startup sequence on HMI.

    In air-conditioning we need to remove heat from the occupied space which should be maintained at about 23oC to 24oC
•
Since heat flows from higher to lower temperature, the fluid removing heat has to be colder than the space temperature
•
Once the heat is removed, it has to be rejected to something that is colder
•
This requires refrigerating systems which can maintain a body at a temperature below that of the surroundings
    """
    
    print("="*70)
    print("TESTING CHUNKING STRATEGIES")
    print("="*70)
    
    # Test token-based
    print("\n[1] Token-Based Chunking:")
    token_chunker = TokenBasedChunking(chunk_size=100, chunk_overlap=20)
    chunks = token_chunker.chunk(sample_text)
    print(f"  Generated {len(chunks)} chunks")
    for i, chunk in enumerate(chunks, 1):
        print(f"  Chunk {i}: {chunk['tokens']} tokens")
    
    # Test semantic
    print("\n[2] Semantic Chunking:")
    semantic_chunker = SemanticChunking(max_chunk_size=200)
    chunks = semantic_chunker.chunk(sample_text)
    print(f"  Generated {len(chunks)} chunks")
    for i, chunk in enumerate(chunks, 1):
        print(f"  Chunk {i}: {chunk['tokens']} tokens")
        print(f"    Preview: {chunk['text'][:80]}...")
    
    # Test hybrid
    print("\n[3] Hybrid Chunking:")
    hybrid_chunker = HybridChunking()
    chunks = hybrid_chunker.chunk(sample_text)
    print(f"  Generated {len(chunks)} chunks")
    print(f"  Method: {chunks[0].get('chunking_method', 'token-based')}")