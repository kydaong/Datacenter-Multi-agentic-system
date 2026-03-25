"""
Vectorization Configuration
Central config for document processing and embedding
"""

import os
from dotenv import load_dotenv

load_dotenv()

class VectorizationConfig:
    """Configuration for vectorization pipeline"""
    
    # Qdrant settings
    QDRANT_HOST = os.getenv('QDRANT_HOST', 'localhost')
    QDRANT_PORT = int(os.getenv('QDRANT_PORT', 6333))
    
    # Embedding model
    EMBEDDING_MODEL = os.getenv('EMBEDDING_MODEL', 'all-MiniLM-L6-v2')
    EMBEDDING_DIMENSION = 384  # for all-MiniLM-L6-v2
    
    # Chunking strategy
    CHUNK_SIZE = 1000  # tokens
    CHUNK_OVERLAP = 200  # tokens
    
    # Collections
    COLLECTIONS = {
        'sops': {
            'name': 'sops',
            'description': 'Standard Operating Procedures',
            'criticality': 'HIGH'
        },
        'equipment_manuals': {
            'name': 'equipment_manuals',
            'description': 'Equipment manuals and technical documentation',
            'criticality': 'HIGH'
        },
        'regulations': {
            'name': 'regulations',
            'description': 'Compliance regulations (NEA, ASHRAE, ISO)',
            'criticality': 'HIGH'
        },
        'kpi_definitions': {
            'name': 'kpi_definitions',
            'description': 'KPI definitions and calculation methods',
            'criticality': 'MEDIUM'
        }
    }
    
    # Document directories
    DOCUMENT_PATHS = {
        'sops': '../documents/sops',
        'chillers': '../documents/manuals/chillers',
        'pumps': '../documents/manuals/pumps',
        'towers': '../documents/manuals/cooling_towers',
        'ahu': '../documents/manuals/ahu',
        'regulations': '../documents/regulations',
        'kpis': '../documents/kpi_definitions'
    }
    
    # Processing settings
    BATCH_SIZE = 100  # for embedding generation
    UPLOAD_BATCH_SIZE = 100  # for Qdrant upload


config = VectorizationConfig()