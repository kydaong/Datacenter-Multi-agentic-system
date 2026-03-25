"""
Document Processor
Handles PDF extraction and text processing
"""

import pdfplumber
from pathlib import Path
from typing import List, Dict, Optional
try:
    from .metadata_extractor import MetadataExtractor
    from .chunking_strategies import HybridChunking
except ImportError:
    from metadata_extractor import MetadataExtractor
    from chunking_strategies import HybridChunking

class DocumentProcessor:
    """
    Process documents: extract text, chunk, add metadata
    """
    
    def __init__(self, chunking_strategy=None):
        """
        Initialize processor
        
        Args:
            chunking_strategy: Chunking strategy (defaults to HybridChunking)
        """
        
        self.metadata_extractor = MetadataExtractor()
        self.chunking_strategy = chunking_strategy or HybridChunking()
            
    def extract_text_from_pdf(self, pdf_path: str) -> List[Dict]:
        """
        Extract text from PDF with page-level metadata
        
        Args:
            pdf_path: Path to PDF file
        
        Returns:
            List of page dictionaries
        """
        
        pages = []
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for i, page in enumerate(pdf.pages, start=1):
                    text = page.extract_text()
                    
                    if text:
                        pages.append({
                            'page_number': i,
                            'text': text.strip(),
                            'total_pages': len(pdf.pages)
                        })
            
            return pages
            
        except Exception as e:
            print(f"❌ Error extracting {pdf_path}: {e}")
            return []
    
    def process_document(
        self,
        filepath: str,
        document_type: str,
        equipment_type: Optional[str] = None,
        criticality: Optional[str] = None
    ) -> List[Dict]:
        """
        Full pipeline: extract → chunk → add metadata
        
        Args:
            filepath: Path to document
            document_type: Type (SOP, MANUAL, REGULATION, KPI)
            equipment_type: Equipment category
            criticality: Criticality level
        
        Returns:
            List of chunks ready for embedding
        """
        
        # Extract filename metadata
        filename_meta = self.metadata_extractor.extract_from_filename(filepath)
        
        # Extract text from PDF
        pages = self.extract_text_from_pdf(filepath)
        
        if not pages:
            return []
        
        # Process each page
        all_chunks = []
        filename = Path(filepath).name
        
        for page in pages:
            # Extract content metadata
            content_meta = self.metadata_extractor.extract_from_content(page['text'])
            
            # Override metadata
            override_meta = {
                'source': filename,
                'page': page['page_number'],
                'total_pages': page['total_pages'],
                'document_type': document_type
            }
            
            if equipment_type:
                override_meta['equipment_type'] = equipment_type
            
            if criticality:
                override_meta['criticality'] = criticality
            
            # Merge all metadata
            merged_meta = self.metadata_extractor.merge_metadata(
                filename_meta,
                content_meta,
                override_meta
            )
            
            # Chunk the page
            chunks = self.chunking_strategy.chunk(page['text'], merged_meta)
            all_chunks.extend(chunks)
        
        return all_chunks
    
    def process_directory(
        self,
        directory: str,
        document_type: str,
        equipment_type: Optional[str] = None,
        criticality: Optional[str] = 'MEDIUM'
    ) -> List[Dict]:
        """
        Process all PDFs in a directory
        
        Args:
            directory: Directory path
            document_type: Document type
            equipment_type: Equipment category
            criticality: Criticality level
        
        Returns:
            All chunks from all documents
        """
        
        pdf_files = list(Path(directory).glob('**/*.pdf'))
        
        if not pdf_files:
            print(f"⚠️  No PDF files found in {directory}")
            return []
        
        print(f"Found {len(pdf_files)} PDF files in {directory}")
        
        all_chunks = []
        
        for pdf_file in pdf_files:
            print(f"  Processing: {pdf_file.name}")
            chunks = self.process_document(
                str(pdf_file),
                document_type=document_type,
                equipment_type=equipment_type,
                criticality=criticality
            )
            all_chunks.extend(chunks)
        
        print(f"✅ Generated {len(all_chunks)} total chunks")
        
        return all_chunks


# Example usage
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING DOCUMENT PROCESSOR")
    print("="*70)
    
    processor = DocumentProcessor()
    
    # Test single document (if exists)
    BASE_DIR = Path(__file__).parent.parent
    test_file = BASE_DIR / "documents" / "SS 591 Redline Chilled Water System Efficiency (1).pdf"

    if test_file.exists():
        print(f"\nProcessing: {test_file.name}")
        
        # For .txt files, we'd need to adapt the processor
        # For now, let's just show the structure
        print("  (Document processor is ready for PDF files)")
    else:
        print(f"\n⚠️  Test file not found: {test_file}")
        print("  Add PDF files to ../documents/sops/ to test")