#!/usr/bin/env python3
"""
Verifier/Checker for Zero-Loss Mapping Workflow
Validates LLM outputs for completeness and accuracy
"""

import json
import re
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
import difflib


class MappingVerifier:
    def __init__(self, batch_file: str, story_json: str = "story.json"):
        """
        Initialize verifier with batch data and original story
        
        Args:
            batch_file: Path to the batch JSON file
            story_json: Path to the original story.json
        """
        self.batch_file = Path(batch_file)
        self.story_json = Path(story_json)
        self.batch_data = self._load_batch_data()
        self.story_data = self._load_story_data()
        self.errors = []
        self.warnings = []
        
    def _load_batch_data(self) -> Dict[str, Any]:
        """Load batch data from JSON"""
        with open(self.batch_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _load_story_data(self) -> Dict[str, Any]:
        """Load original story data"""
        with open(self.story_json, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def parse_markdown_table(self, markdown_response: str) -> List[Dict[str, str]]:
        """Parse markdown table from LLM response"""
        rows = []
        lines = markdown_response.strip().split('\n')
        
        # Find table start
        table_started = False
        headers = []
        
        for line in lines:
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
            
            # Detect header row
            if '|' in line and 'UID' in line and 'Raw Sentence' in line:
                headers = [h.strip() for h in line.split('|') if h.strip()]
                table_started = True
                continue
            
            # Skip separator row
            if table_started and re.match(r'^[\|\s\-]+$', line):
                continue
            
            # Parse data rows
            if table_started and '|' in line:
                values = [v.strip() for v in line.split('|') if v.strip()]
                
                if len(values) >= len(headers):
                    row_dict = {}
                    for i, header in enumerate(headers):
                        if i < len(values):
                            row_dict[header] = values[i]
                    
                    # Ensure we have at least UID and Raw Sentence
                    if 'UID' in row_dict and 'Raw Sentence' in row_dict:
                        rows.append(row_dict)
        
        return rows
    
    def verify_uid_completeness(self, parsed_rows: List[Dict[str, str]]) -> Tuple[bool, List[str]]:
        """Check if all UIDs from the batch are present in the response"""
        expected_uids = [unit['uid'] for unit in self.batch_data['units']]
        found_uids = [row.get('UID', '') for row in parsed_rows]
        
        missing_uids = set(expected_uids) - set(found_uids)
        duplicate_uids = [uid for uid in found_uids if found_uids.count(uid) > 1]
        extra_uids = set(found_uids) - set(expected_uids)
        
        errors = []
        if missing_uids:
            errors.append(f"Missing UIDs: {sorted(missing_uids)}")
            self.errors.append(("missing_uids", sorted(missing_uids)))
        
        if duplicate_uids:
            errors.append(f"Duplicate UIDs: {sorted(set(duplicate_uids))}")
            self.errors.append(("duplicate_uids", sorted(set(duplicate_uids))))
        
        if extra_uids:
            errors.append(f"Extra UIDs not in batch: {sorted(extra_uids)}")
            self.warnings.append(("extra_uids", sorted(extra_uids)))
        
        return len(missing_uids) == 0 and len(duplicate_uids) == 0, errors
    
    def verify_text_accuracy(self, parsed_rows: List[Dict[str, str]]) -> Tuple[bool, List[str]]:
        """Verify that raw sentences match exactly"""
        # Create lookup for original text
        uid_to_text = {unit['uid']: unit['text'] for unit in self.batch_data['units']}
        
        errors = []
        text_mismatches = []
        
        for row in parsed_rows:
            uid = row.get('UID', '')
            provided_text = row.get('Raw Sentence', '')
            
            if uid in uid_to_text:
                original_text = uid_to_text[uid]
                
                # Normalize for comparison (handle minor whitespace differences)
                original_normalized = ' '.join(original_text.split())
                provided_normalized = ' '.join(provided_text.split())
                
                if original_normalized != provided_normalized:
                    # Calculate similarity
                    similarity = difflib.SequenceMatcher(None, original_normalized, provided_normalized).ratio()
                    
                    text_mismatches.append({
                        'uid': uid,
                        'original': original_text,
                        'provided': provided_text,
                        'similarity': similarity
                    })
                    
                    if similarity < 0.95:  # Major mismatch
                        self.errors.append(("text_mismatch", uid, similarity))
                    else:  # Minor mismatch
                        self.warnings.append(("text_minor_mismatch", uid, similarity))
        
        if text_mismatches:
            for mismatch in text_mismatches:
                if mismatch['similarity'] < 0.95:
                    errors.append(f"Text mismatch for {mismatch['uid']} (similarity: {mismatch['similarity']:.2f})")
        
        return len([m for m in text_mismatches if m['similarity'] < 0.95]) == 0, errors
    
    def verify_table_structure(self, parsed_rows: List[Dict[str, str]]) -> Tuple[bool, List[str]]:
        """Verify table has all required columns"""
        required_columns = ['UID', 'Raw Sentence', 'Narrative Purpose']
        optional_columns = ['Characters', 'Locations', 'Key Items/Concepts', 'Links']
        
        errors = []
        warnings = []
        
        if not parsed_rows:
            errors.append("No valid table rows found")
            return False, errors
        
        # Check first row for column structure
        first_row = parsed_rows[0]
        missing_required = [col for col in required_columns if col not in first_row]
        missing_optional = [col for col in optional_columns if col not in first_row]
        
        if missing_required:
            errors.append(f"Missing required columns: {missing_required}")
            self.errors.append(("missing_columns", missing_required))
        
        if missing_optional:
            warnings.append(f"Missing optional columns: {missing_optional}")
            self.warnings.append(("missing_optional_columns", missing_optional))
        
        # Check for empty required fields
        empty_fields = []
        for row in parsed_rows:
            for col in required_columns:
                if col in row and not row[col].strip():
                    empty_fields.append(f"{row.get('UID', 'Unknown')} - {col}")
        
        if empty_fields:
            errors.append(f"Empty required fields: {empty_fields[:5]}...")  # Show first 5
            self.errors.append(("empty_required_fields", len(empty_fields)))
        
        return len(missing_required) == 0 and len(empty_fields) == 0, errors
    
    def generate_report(self, parsed_rows: List[Dict[str, str]], llm_response: str) -> Dict[str, Any]:
        """Generate comprehensive verification report"""
        # Run all checks
        uid_complete, uid_errors = self.verify_uid_completeness(parsed_rows)
        text_accurate, text_errors = self.verify_text_accuracy(parsed_rows)
        structure_valid, structure_errors = self.verify_table_structure(parsed_rows)
        
        # Calculate statistics
        total_expected = len(self.batch_data['units'])
        total_found = len(parsed_rows)
        
        report = {
            "batch_id": self.batch_data['batch_id'],
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_expected_units": total_expected,
                "total_found_units": total_found,
                "completeness_rate": (total_found / total_expected * 100) if total_expected > 0 else 0,
                "all_checks_passed": uid_complete and text_accurate and structure_valid
            },
            "checks": {
                "uid_completeness": {
                    "passed": uid_complete,
                    "errors": uid_errors
                },
                "text_accuracy": {
                    "passed": text_accurate,
                    "errors": text_errors
                },
                "table_structure": {
                    "passed": structure_valid,
                    "errors": structure_errors
                }
            },
            "detailed_errors": self.errors,
            "warnings": self.warnings,
            "parsed_row_count": len(parsed_rows),
            "recommendation": self._get_recommendation(uid_complete, text_accurate, structure_valid)
        }
        
        return report
    
    def _get_recommendation(self, uid_complete: bool, text_accurate: bool, structure_valid: bool) -> str:
        """Get recommendation based on verification results"""
        if uid_complete and text_accurate and structure_valid:
            return "ACCEPT: All checks passed, safe to merge"
        elif uid_complete and structure_valid and not text_accurate:
            if all(w[0] == "text_minor_mismatch" for w in self.warnings if w[0].startswith("text")):
                return "ACCEPT_WITH_WARNINGS: Minor text differences, review recommended"
            else:
                return "REJECT: Significant text mismatches detected"
        elif not uid_complete:
            return "REJECT: Missing or duplicate UIDs, regenerate response"
        else:
            return "REJECT: Structural issues, regenerate response"
    
    def save_report(self, report: Dict[str, Any], output_file: Optional[str] = None):
        """Save verification report"""
        if output_file is None:
            output_file = f"verification_{self.batch_data['batch_id']}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        print(f"✓ Verification report saved to {output_file}")
    
    def verify_response(self, llm_response: str) -> Dict[str, Any]:
        """Main verification method"""
        # Parse the response
        parsed_rows = self.parse_markdown_table(llm_response)
        
        # Generate report
        report = self.generate_report(parsed_rows, llm_response)
        
        # Print summary
        print(f"\nVerification Summary for {self.batch_data['batch_id']}:")
        print(f"- Expected units: {report['summary']['total_expected_units']}")
        print(f"- Found units: {report['summary']['total_found_units']}")
        print(f"- Completeness: {report['summary']['completeness_rate']:.1f}%")
        print(f"- Recommendation: {report['recommendation']}")
        
        return report


def main():
    """Example usage"""
    # This would normally be called by the orchestrator with actual LLM response
    
    # Create sample response for testing
    sample_response = """
| UID | Raw Sentence | Narrative Purpose | Characters | Locations | Key Items/Concepts | Links |
|-----|--------------|-------------------|------------|-----------|-------------------|-------|
| CH01-P000-S000 | Chapter 1: The Misfits Assemble at Nexus Prime Factory | Chapter introduction | N/A | Nexus Prime Factory | N/A | N/A |
    """
    
    # Verify the response
    verifier = MappingVerifier("batches/BATCH_0001.json")
    report = verifier.verify_response(sample_response)
    verifier.save_report(report)


if __name__ == "__main__":
    print("Verifier module ready. This will be called by the orchestrator with actual LLM responses.")
    print("Run orchestrator.py to process the full pipeline.")