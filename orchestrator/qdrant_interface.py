"""
Qdrant Vector Database Interface
Enables agents to search SOPs, manuals, regulations
"""

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue
from sentence_transformers import SentenceTransformer
import os
from dotenv import load_dotenv
from typing import List, Dict, Optional

load_dotenv()

class QdrantInterface:
    """
    Interface to Qdrant vector database for knowledge retriZZeval
    """
    
    def __init__(self):
        self.client = QdrantClient(
            host=os.getenv('QDRANT_HOST', 'localhost'),  #set fallback value in case env key not retrieved
            port=int(os.getenv('QDRANT_PORT', 6333)),
            api_key=os.getenv('QDRANT_API_KEY'),
            https=False
        )

        # Load embedding model
        model_name = os.getenv('EMBEDDING_MODEL', 'all-MiniLM-L6-v2')
        self.encoder = SentenceTransformer(model_name)
        
        print(f"Qdrant connected: {os.getenv('QDRANT_HOST', 'localhost')}:{os.getenv('QDRANT_PORT', 6333)}")
    
    def search_sops(
        self, 
        query: str, 
        equipment_type: Optional[str] = None,
        top_k: int = 3
    ) -> List[Dict]:
        """
        Search Standard Operating Procedures
        
        Args:
            query: Natural language query (e.g., "chiller startup procedure")
            equipment_type: Filter by equipment (e.g., "chiller", "pump")
            top_k: Number of results to return
        
        Returns:
            List of relevant SOP sections with metadata
        """
        
        # Generate query embedding
        query_vector = self.encoder.encode(query).tolist()
        
        # Build filter
        search_filter = None
        if equipment_type:
            search_filter = Filter(
                must=[
                    FieldCondition(
                        key="equipment_type",
                        match=MatchValue(value=equipment_type)
                    )
                ]
            )
        
        # Search
        response = self.client.search(
            collection_name="sops",
            query_vector=query_vector,
            query_filter=search_filter,
            limit=top_k,
            with_payload=True
        )

        # Format results
        formatted_results = []
        for hit in response:
            formatted_results.append({
                'text': hit.payload.get('text'),
                'source': hit.payload.get('source'),
                'page': hit.payload.get('page'),
                'equipment_type': hit.payload.get('equipment_type'),
                'section': hit.payload.get('section'),
                'score': hit.score
            })
        
        return formatted_results
    
    def search_equipment_manuals(
        self,
        query: str,
        equipment_id: Optional[str] = None,
        top_k: int = 3
    ) -> List[Dict]:
        """
        Search equipment manuals
        
        Args:
            query: Query (e.g., "oil change procedure")
            equipment_id: Specific equipment (e.g., "Chiller-1")
            top_k: Number of results
        
        Returns:
            Relevant manual sections
        """
        
        query_vector = self.encoder.encode(query).tolist()
        
        search_filter = None
        if equipment_id:
            search_filter = Filter(
                must=[
                    FieldCondition(
                        key="equipment_id",
                        match=MatchValue(value=equipment_id)
                    )
                ]
            )
        
        response = self.client.search(
            collection_name="equipment_manuals",
            query_vector=query_vector,
            query_filter=search_filter,
            limit=top_k,
            with_payload=True
        )

        formatted_results = []
        for hit in response:
            formatted_results.append({
                'text': hit.payload.get('text'),
                'source': hit.payload.get('source'),
                'page': hit.payload.get('page'),
                'equipment_id': hit.payload.get('equipment_id'),
                'score': hit.score
            })
        
        return formatted_results
    
    def search_regulations(
        self,
        query: str,
        top_k: int = 3
    ) -> List[Dict]:
        """
        Search compliance regulations
        
        Args:
            query: Compliance question
            top_k: Number of results
        
        Returns:
            Relevant regulation sections
        """
        
        query_vector = self.encoder.encode(query).tolist()
        
        response = self.client.search(
            collection_name="regulations",
            query_vector=query_vector,
            limit=top_k,
            with_payload=True
        )

        formatted_results = []
        for hit in response:
            formatted_results.append({
                'text': hit.payload.get('text'),
                'source': hit.payload.get('source'),
                'regulation_type': hit.payload.get('regulation_type'),
                'score': hit.score
            })
        
        return formatted_results
    
    def search_kpi_definitions(
        self,
        query: str,
        top_k: int = 2
    ) -> List[Dict]:
        """
        Search KPI definitions (PUE, WUE, IPLV, etc.)
        
        Args:
            query: KPI question
            top_k: Number of results
        
        Returns:
            KPI definition and calculation method
        """
        
        query_vector = self.encoder.encode(query).tolist()
        
        response = self.client.search(
            collection_name="kpi_definitions",
            query_vector=query_vector,
            limit=top_k,
            with_payload=True
        )

        formatted_results = []
        for hit in response:
            formatted_results.append({
                'text': hit.payload.get('text'),
                'kpi_name': hit.payload.get('kpi_name'),
                'standard': hit.payload.get('standard'),
                'score': hit.score
            })
        
        return formatted_results


# Singleton instance
qdrant = QdrantInterface()

# call function
if __name__ == "__main__":
    
    # Test SOP search
    print("\n[TEST 1] Searching SOPs...")
    results = qdrant.search_sops("chiller startup procedure after maintenance", equipment_type="chiller")
    
    for i, result in enumerate(results, 1):
        print(f"\nResult {i} (score: {result['score']:.3f}):")
        print(f"  Source: {result['source']}")
        print(f"  Section: {result['section']}")
        print(f"  Text: {result['text'][:200]}...")
    
    # Test manual search
    print("\n[TEST 2] Searching Equipment Manuals...")
    results = qdrant.search_equipment_manuals("oil pressure alarm troubleshooting")
    
    for i, result in enumerate(results, 1):
        print(f"\nResult {i} (score: {result['score']:.3f}):")
        print(f"  Source: {result['source']}")
        print(f"  Text: {result['text'][:200]}...")
    
    # Test regulation search
    print("\n[TEST 3] Searching Regulations...")
    results = qdrant.search_regulations("Singapore energy efficiency requirements datacenters")
    
    for i, result in enumerate(results, 1):
        print(f"\nResult {i} (score: {result['score']:.3f}):")
        print(f"  Source: {result['source']}")
        print(f"  Text: {result['text'][:200]}...")