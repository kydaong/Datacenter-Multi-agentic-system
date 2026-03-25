"""
Metadata Extractor
Extracts metadata from documents for enhanced search
"""

from pathlib import Path
from typing import Dict, Optional
import re

class MetadataExtractor:
    """
    Extract metadata from document filename and content
    """
    
    @staticmethod
    def extract_from_filename(filepath: str) -> Dict:
        """
        Extract metadata from filename
        
        Examples:
            SOP-CP-001_Chiller_Startup.pdf → {type: 'SOP', code: 'CP-001', equipment: 'chiller'}
            Trane_RTWD_Service_Manual.pdf → {manufacturer: 'Trane', model: 'RTWD'}
        """
        
        filename = Path(filepath).stem
        metadata = {}
        
        # SOP pattern: SOP-XX-NNN
        sop_match = re.match(r'SOP-([A-Z]+)-(\d+)', filename)
        if sop_match:
            metadata['document_type'] = 'SOP'
            metadata['sop_category'] = sop_match.group(1)
            metadata['sop_number'] = sop_match.group(2)
        
        # Equipment detection
        equipment_keywords = {
            'chiller': ['chiller', 'rtwd', 'centrifugal', 'screw'],
            'pump': ['pump', 'pchwp', 'schwp', 'cwp'],
            'tower': ['tower', 'cooling_tower', 'ct', 'bac'],
            'ahu': ['ahu', 'air_handler', 'air_handling']
        }
        
        filename_lower = filename.lower()
        for equipment_type, keywords in equipment_keywords.items():
            if any(kw in filename_lower for kw in keywords):
                metadata['equipment_type'] = equipment_type
                break
        
        # Manufacturer detection
        manufacturers = ['trane', 'carrier', 'york', 'bac', 'grundfos', 'armstrong']
        for mfr in manufacturers:
            if mfr in filename_lower:
                metadata['manufacturer'] = mfr.title()
                break
        
        return metadata
    
    @staticmethod
    def extract_from_content(text: str) -> Dict:
        """
        Extract metadata from document content
        
        Looks for:
        - Document dates
        - Revision numbers
        - Equipment IDs
        - Criticality levels
        """
        
        metadata = {}
        
        # Date pattern: 2024-01-15, 15/01/2024, Jan 15, 2024
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'[A-Z][a-z]+\s+\d{1,2},?\s+\d{4}'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                metadata['document_date'] = match.group(0)
                break
        
        # Revision pattern: Rev 3.0, Revision: 2.1
        revision_match = re.search(r'Rev(?:ision)?[:\s]+(\d+\.?\d*)', text, re.IGNORECASE)
        if revision_match:
            metadata['revision'] = revision_match.group(1)
        
        # Criticality level
        criticality_keywords = {
            'HIGH': ['critical', 'mandatory', 'required', 'must'],
            'MEDIUM': ['recommended', 'should', 'important'],
            'LOW': ['optional', 'may', 'suggested']
        }
        
        text_lower = text.lower()
        for level, keywords in criticality_keywords.items():
            if any(kw in text_lower for kw in keywords):
                metadata['criticality'] = level
                break
        
        # Equipment ID pattern: Chiller-1, PCHWP-2, CT-3
        equipment_match = re.search(r'(Chiller|PCHWP|SCHWP|CWP|CT)-(\d+)', text)
        if equipment_match:
            metadata['equipment_id'] = equipment_match.group(0)
        
        return metadata
    
    @staticmethod
    def merge_metadata(
        filename_meta: Dict,
        content_meta: Dict,
        override_meta: Optional[Dict] = None
    ) -> Dict:
        """
        Merge metadata from multiple sources
        
        Priority: override_meta > content_meta > filename_meta
        """
        
        merged = {}
        merged.update(filename_meta)
        merged.update(content_meta)
        
        if override_meta:
            merged.update(override_meta)
        
        return merged


# Example usage
if __name__ == "__main__":
    
    extractor = MetadataExtractor()
    
    # Test filename extraction
    print("="*70)
    print("TESTING METADATA EXTRACTION")
    print("="*70)
    
    test_files = [
        "SOP-CP-001_Chiller_Startup.pdf",
        "Trane_RTWD_Service_Manual.pdf",
        "NEA_Energy_Efficiency_Requirements.pdf"
    ]
    
    for filename in test_files:
        print(f"\nFile: {filename}")
        metadata = extractor.extract_from_filename(filename)
        print(f"  Metadata: {metadata}")
    
    # Test content extraction
    sample_text = """
    STANDARD OPERATING PROCEDURE
    SOP-CP-001: Chiller Startup
    Revision: 3.0
    Date: 2024-01-15
    Equipment: Chiller-1
    Criticality: CRITICAL
    
    This procedure is MANDATORY for all chiller startups.
    """
    
    print("\n" + "="*70)
    print("Content Metadata:")
    content_meta = extractor.extract_from_content(sample_text)
    print(content_meta)