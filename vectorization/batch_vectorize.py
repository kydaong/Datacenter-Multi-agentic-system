"""
Batch Vectorization Script
Processes all documents and uploads to Qdrant
Main execution script 
"""

from pathlib import Path

try:
    from .config import config
    from .document_processor import DocumentProcessor
    from .embeddings_generator import EmbeddingsGenerator
    from .qdrant_manager import QdrantManager
except ImportError:
    from config import config
    from document_processor import DocumentProcessor
    from embeddings_generator import EmbeddingsGenerator
    from qdrant_manager import QdrantManager

BASE_DIR = Path(__file__).parent.parent

def batch_vectorize_all():
    """
    Process all document directories and upload to Qdrant
    """
    
    print("="*70)
    print("BATCH DOCUMENT VECTORIZATION")
    print("="*70)
    
    # Initialize components
    processor = DocumentProcessor()
    embeddings_gen = EmbeddingsGenerator()
    qdrant_mgr = QdrantManager()
    
    # Document configurations — using absolute paths from project root
    document_configs = [
        {
            'directory': str(BASE_DIR / 'documents' / 'sops'),
            'collection': 'sops',
            'document_type': 'SOP',
            'equipment_type': 'general',
            'criticality': 'HIGH'
        },
        {
            'directory': str(BASE_DIR / 'documents' / 'manuals' / 'chillers'),
            'collection': 'equipment_manuals',
            'document_type': 'MANUAL',
            'equipment_type': 'chiller',
            'criticality': 'HIGH'
        },
        {
            'directory': str(BASE_DIR / 'documents' / 'manuals' / 'pumps'),
            'collection': 'equipment_manuals',
            'document_type': 'MANUAL',
            'equipment_type': 'pump',
            'criticality': 'MEDIUM'
        },
        {
            'directory': str(BASE_DIR / 'documents' / 'manuals' / 'cooling_towers'),
            'collection': 'equipment_manuals',
            'document_type': 'MANUAL',
            'equipment_type': 'tower',
            'criticality': 'MEDIUM'
        },
        {
            'directory': str(BASE_DIR / 'documents' / 'manuals' / 'ahu'),
            'collection': 'equipment_manuals',
            'document_type': 'MANUAL',
            'equipment_type': 'ahu',
            'criticality': 'LOW'
        },
        {
            'directory': str(BASE_DIR / 'documents' / 'regulations'),
            'collection': 'regulations',
            'document_type': 'REGULATION',
            'equipment_type': 'general',
            'criticality': 'HIGH'
        },
        {
            'directory': str(BASE_DIR / 'documents' / 'kpi_definitions'),
            'collection': 'kpi_definitions',
            'document_type': 'KPI',
            'equipment_type': 'general',
            'criticality': 'MEDIUM'
        }
    ]
    
    # Track which collections we've created
    created_collections = set()
    
    # Process each configuration
    for i, cfg in enumerate(document_configs, 1):
        print(f"\n{'='*70}")
        print(f"[{i}/{len(document_configs)}] Processing: {cfg['directory']}")
        print(f"{'='*70}")
        
        # Check directory exists
        if not Path(cfg['directory']).exists():
            print(f"⚠️  Directory not found: {cfg['directory']}")
            print(f"   Creating directory...")
            Path(cfg['directory']).mkdir(parents=True, exist_ok=True)
            print(f"   No PDFs to process, skipping...")
            continue
        
        # Create collection (once per collection)
        if cfg['collection'] not in created_collections:
            qdrant_mgr.create_collection(
                collection_name=cfg['collection'],
                recreate=False  # Don't delete existing data
            )
            created_collections.add(cfg['collection'])
        
        # Process directory
        chunks = processor.process_directory(
            directory=cfg['directory'],
            document_type=cfg['document_type'],
            equipment_type=cfg['equipment_type'],
            criticality=cfg['criticality']
        )
        
        if not chunks:
            print(f"  No chunks generated, skipping...")
            continue
        
        # Generate embeddings
        chunks_with_embeddings = embeddings_gen.generate_for_chunks(chunks)
        
        # Upload to Qdrant
        qdrant_mgr.upload_chunks(cfg['collection'], chunks_with_embeddings)
    
    # Print final summary
    print("\n" + "="*70)
    print(" BATCH VECTORIZATION COMPLETE!")
    print("="*70)
    
    qdrant_mgr.print_summary()


if __name__ == "__main__":
    batch_vectorize_all()