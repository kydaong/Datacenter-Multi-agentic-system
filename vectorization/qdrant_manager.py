"""
Qdrant Manager
Manages Qdrant collections and vector operations
"""

from qdrant_client import QdrantClient
from qdrant_client.models import (
    PointStruct,
    VectorParams,
    Distance,
    PayloadSchemaType,
    Filter,
    FieldCondition,
    MatchValue
)
from typing import List, Dict, Optional
from tqdm import tqdm
import uuid
try:
    from .config import config
except ImportError:
    from config import config

class QdrantManager:
    """
    Manage Qdrant collections and operations
    """
    
    def __init__(self, host: str = None, port: int = None):
        """
        Initialize Qdrant manager
        
        Args:
            host: Qdrant host (defaults to config)
            port: Qdrant port (defaults to config)
        """
        
        self.host = host or config.QDRANT_HOST
        self.port = port or config.QDRANT_PORT
        
        print(f"Connecting to Qdrant at {self.host}:{self.port}...")
        self.client = QdrantClient(host=self.host, port=self.port)
        
        print(f"✅ Connected to Qdrant")
    
    def create_collection(
        self,
        collection_name: str,
        vector_size: int = None,
        distance: Distance = Distance.COSINE,
        recreate: bool = False
    ):
        """
        Create Qdrant collection
        
        Args:
            collection_name: Collection name
            vector_size: Embedding dimension (defaults to config)
            distance: Distance metric
            recreate: Delete existing collection first
        """
        
        vector_size = vector_size or config.EMBEDDING_DIMENSION
        
        # Check if exists
        collections = self.client.get_collections().collections
        exists = any(c.name == collection_name for c in collections)
        
        if exists and recreate:
            print(f"Deleting existing collection: {collection_name}")
            self.client.delete_collection(collection_name)
            exists = False
        
        if not exists:
            print(f"Creating collection: {collection_name}")
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=vector_size,
                    distance=distance
                )
            )
            
            # Create payload indexes for faster filtering
            self._create_indexes(collection_name)
            
            print(f"✅ Collection '{collection_name}' created")
        else:
            print(f"⚠️  Collection '{collection_name}' already exists")
    
    def _create_indexes(self, collection_name: str):
        """
        Create payload indexes for common fields
        
        Args:
            collection_name: Collection name
        """
        
        index_fields = [
            'equipment_type',
            'document_type',
            'criticality',
            'source',
            'manufacturer'
        ]
        
        for field in index_fields:
            try:
                self.client.create_payload_index(
                    collection_name=collection_name,
                    field_name=field,
                    field_schema=PayloadSchemaType.KEYWORD
                )
            except Exception as e:
                # Index might already exist
                pass
    
    def upload_chunks(
        self,
        collection_name: str,
        chunks: List[Dict],
        batch_size: int = None
    ):
        """
        Upload chunks with embeddings to Qdrant
        
        Args:
            collection_name: Target collection
            chunks: Chunks with 'embedding' field
            batch_size: Upload batch size (defaults to config)
        """
        
        batch_size = batch_size or config.UPLOAD_BATCH_SIZE
        
        print(f"\nUploading {len(chunks)} chunks to '{collection_name}'...")
        
        # Prepare points
        points = []
        
        for chunk in chunks:
            # Validate embedding exists
            if 'embedding' not in chunk:
                print(f"⚠️  Chunk missing embedding, skipping")
                continue
            
            # Create payload (exclude embedding from payload)
            payload = {k: v for k, v in chunk.items() if k != 'embedding'}
            
            # Create point
            point = PointStruct(
                id=str(uuid.uuid4()),
                vector=chunk['embedding'],
                payload=payload
            )
            
            points.append(point)
        
        # Upload in batches
        for i in tqdm(range(0, len(points), batch_size), desc="Uploading batches"):
            batch = points[i:i + batch_size]
            
            try:
                self.client.upsert(
                    collection_name=collection_name,
                    points=batch
                )
            except Exception as e:
                print(f"❌ Error uploading batch {i//batch_size + 1}: {e}")
        
        print(f" Uploaded {len(points)} points to '{collection_name}'")
    
    def search(
        self,
        collection_name: str,
        query_vector: List[float],
        limit: int = 5,
        filters: Optional[Dict] = None
    ) -> List[Dict]:
        """
        Search collection with vector and optional filters
        
        Args:
            collection_name: Collection to search
            query_vector: Query embedding
            limit: Number of results
            filters: Optional filters (e.g., {'equipment_type': 'chiller'})
        
        Returns:
            Search results with payload
        """
        
        # Build filter
        search_filter = None
        if filters:
            conditions = []
            for key, value in filters.items():
                conditions.append(
                    FieldCondition(
                        key=key,
                        match=MatchValue(value=value)
                    )
                )
            
            search_filter = Filter(must=conditions)
        
        # Search
        results = self.client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            query_filter=search_filter,
            limit=limit,
            with_payload=True
        )
        
        # Format results
        formatted_results = []
        for hit in results:
            formatted_results.append({
                'id': hit.id,
                'score': hit.score,
                'payload': hit.payload
            })
        
        return formatted_results
    
    def get_collection_info(self, collection_name: str) -> Dict:
        """
        Get collection information
        
        Args:
            collection_name: Collection name
        
        Returns:
            Collection info
        """
        
        info = self.client.get_collection(collection_name)
        
        count = getattr(info, 'points_count', None) or getattr(info, 'vectors_count', 0)
        return {
            'name': collection_name,
            'points_count': count,
            'vector_size': info.config.params.vectors.size,
            'distance': str(info.config.params.vectors.distance)
        }
    
    def list_collections(self) -> List[str]:
        """
        List all collections
        
        Returns:
            List of collection names
        """
        
        collections = self.client.get_collections().collections
        return [c.name for c in collections]
    
    def delete_collection(self, collection_name: str):
        """
        Delete collection
        
        Args:
            collection_name: Collection to delete
        """
        
        print(f"Deleting collection: {collection_name}")
        self.client.delete_collection(collection_name)
        print(f" Collection '{collection_name}' deleted")
    
    def print_summary(self):
        """
        Print summary of all collections
        """

        print("\n" + "="*70)
        print("QDRANT COLLECTIONS SUMMARY")
        print("="*70)

        collections = self.list_collections()

        if not collections:
            print("  No collections found")
        else:
            for name in collections:
                try:
                    count = self.client.count(collection_name=name).count
                    print(f"  {name:30s}: {count:>10,} vectors")
                except Exception:
                    print(f"  {name:30s}: (collection exists)")

        print("="*70)


# Example usage
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING QDRANT MANAGER")
    print("="*70)
    
    # Initialize manager
    manager = QdrantManager()
    
    # Test 1: Create collection
    print("\n[TEST 1] Create Collection:")
    manager.create_collection(
        collection_name='test_collection',
        recreate=True
    )
    
    # Test 2: List collections
    print("\n[TEST 2] List Collections:")
    collections = manager.list_collections()
    print(f"  Found {len(collections)} collections:")
    for name in collections:
        print(f"    - {name}")
    
    # Test 3: Upload test data
    print("\n[TEST 3] Upload Test Data:")
    
    # Generate dummy embeddings (384 dimensions)
    import numpy as np
    
    test_chunks = [
        {
            'text': 'Chiller startup procedure',
            'source': 'test.pdf',
            'page': 1,
            'equipment_type': 'chiller',
            'embedding': np.random.randn(384).tolist()
        },
        {
            'text': 'Oil heater warm-up required',
            'source': 'test.pdf',
            'page': 1,
            'equipment_type': 'chiller',
            'embedding': np.random.randn(384).tolist()
        }
    ]
    
    manager.upload_chunks('test_collection', test_chunks)
    
    # Test 4: Get collection info
    print("\n[TEST 4] Collection Info:")
    info = manager.get_collection_info('test_collection')
    print(f"  Name: {info['name']}")
    print(f"  Points: {info['points_count']}")
    print(f"  Dimension: {info['vector_size']}")
    
    # Test 5: Search
    print("\n[TEST 5] Search:")
    query_vector = np.random.randn(384).tolist()
    results = manager.search(
        collection_name='test_collection',
        query_vector=query_vector,
        limit=2,
        filters={'equipment_type': 'chiller'}
    )
    
    print(f"  Found {len(results)} results")
    for i, result in enumerate(results, 1):
        print(f"  Result {i}:")
        print(f"    Score: {result['score']:.4f}")
        print(f"    Text: {result['payload']['text']}")
    
    # Print summary
    manager.print_summary()
    
    # Cleanup
    print("\n[CLEANUP] Deleting test collection...")
    manager.delete_collection('test_collection')
    
    print("\n All tests passed!")