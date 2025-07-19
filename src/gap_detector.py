#!/usr/bin/env python3
"""
Gap Detector for Zero-Loss Mapping Pipeline
Automatically surfaces gaps and omissions in story mapping
"""

import json
from pathlib import Path
from typing import Dict, List, Set
from collections import defaultdict


class GapDetector:
    def __init__(self, story_json: str = "story.json", mapping_json: str = "mapping.json"):
        """
        Initialize gap detector
        
        Args:
            story_json: Path to original story data
            mapping_json: Path to generated mapping data
        """
        self.story_json = Path(story_json)
        self.mapping_json = Path(mapping_json)
        
        if self.story_json.exists():
            with open(self.story_json) as f:
                self.story_data = json.load(f)
        else:
            raise FileNotFoundError(f"Story file not found: {story_json}")
            
        if self.mapping_json.exists():
            with open(self.mapping_json) as f:
                mapping_file = json.load(f)
                # Handle nested structure if present
                if 'mapping' in mapping_file:
                    self.mapping_data = mapping_file['mapping']
                else:
                    self.mapping_data = mapping_file
        else:
            raise FileNotFoundError(f"Mapping file not found: {mapping_json}")
    
    def detect_missing_uids(self) -> Dict[str, List[str]]:
        """Detect missing UIDs in the mapping"""
        original_uids = {unit['uid'] for unit in self.story_data['data']}
        mapped_uids = {unit['UID'] for unit in self.mapping_data if 'UID' in unit}
        
        missing_uids = original_uids - mapped_uids
        extra_uids = mapped_uids - original_uids
        
        return {
            'missing_from_mapping': sorted(list(missing_uids)),
            'extra_in_mapping': sorted(list(extra_uids))
        }
    
    def detect_chapter_count_changes(self) -> Dict[int, Dict[str, int]]:
        """Detect changes in chapter unit counts"""
        original_counts = defaultdict(int)
        mapped_counts = defaultdict(int)
        
        # Count original units per chapter
        for unit in self.story_data['data']:
            original_counts[unit['chapter']] += 1
        
        # Count mapped units per chapter
        for unit in self.mapping_data:
            if 'UID' in unit:
                chapter = int(unit['UID'].split('-')[0].replace('CH', ''))
                mapped_counts[chapter] += 1
        
        changes = {}
        for chapter in sorted(set(original_counts.keys()) | set(mapped_counts.keys())):
            orig_count = original_counts[chapter]
            mapped_count = mapped_counts[chapter]
            
            if orig_count != mapped_count:
                changes[chapter] = {
                    'original': orig_count,
                    'mapped': mapped_count,
                    'difference': mapped_count - orig_count
                }
        
        return changes
    
    def detect_text_mismatches(self) -> List[Dict[str, str]]:
        """Detect text mismatches between original and mapped content"""
        mismatches = []
        
        # Create lookup for original text by UID
        original_text = {}
        for unit in self.story_data['data']:
            original_text[unit['uid']] = unit['text']
        
        # Check mapped text against original
        for unit in self.mapping_data:
            if 'UID' in unit and 'Raw Sentence' in unit:
                uid = unit['UID']
                mapped_text = unit['Raw Sentence']
                
                if uid in original_text:
                    original = original_text[uid]
                    if original != mapped_text:
                        mismatches.append({
                            'uid': uid,
                            'original': original,
                            'mapped': mapped_text,
                            'similarity': self._calculate_similarity(original, mapped_text)
                        })
        
        return mismatches
    
    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate simple similarity score between two texts"""
        if not text1 or not text2:
            return 0.0
        
        # Simple character-based similarity
        longer = max(len(text1), len(text2))
        same = sum(c1 == c2 for c1, c2 in zip(text1, text2))
        return same / longer if longer > 0 else 1.0
    
    def generate_gap_report(self) -> Dict:
        """Generate comprehensive gap detection report"""
        missing_uids = self.detect_missing_uids()
        chapter_changes = self.detect_chapter_count_changes()
        text_mismatches = self.detect_text_mismatches()
        
        # Calculate summary statistics
        total_missing = len(missing_uids['missing_from_mapping'])
        total_extra = len(missing_uids['extra_in_mapping'])
        total_chapters_changed = len(chapter_changes)
        total_text_mismatches = len(text_mismatches)
        
        # Determine overall status
        has_critical_issues = total_missing > 0 or total_chapters_changed > 0
        has_warnings = total_extra > 0 or total_text_mismatches > 0
        
        if has_critical_issues:
            status = "CRITICAL_GAPS_DETECTED"
        elif has_warnings:
            status = "WARNINGS_DETECTED"
        else:
            status = "NO_GAPS_DETECTED"
        
        return {
            'status': status,
            'summary': {
                'missing_uids': total_missing,
                'extra_uids': total_extra,
                'chapters_changed': total_chapters_changed,
                'text_mismatches': total_text_mismatches
            },
            'details': {
                'missing_uids': missing_uids,
                'chapter_changes': chapter_changes,
                'text_mismatches': text_mismatches
            },
            'recommendations': self._generate_recommendations(
                total_missing, total_extra, total_chapters_changed, total_text_mismatches
            )
        }
    
    def _generate_recommendations(self, missing: int, extra: int, 
                                chapters_changed: int, mismatches: int) -> List[str]:
        """Generate actionable recommendations based on detected gaps"""
        recommendations = []
        
        if missing > 0:
            recommendations.append(f"❌ CRITICAL: {missing} UIDs missing from mapping - run verifier.py to identify")
        
        if chapters_changed > 0:
            recommendations.append(f"❌ CRITICAL: {chapters_changed} chapters have count changes - check for omissions")
        
        if extra > 0:
            recommendations.append(f"⚠️  WARNING: {extra} extra UIDs in mapping - possible duplicates")
        
        if mismatches > 0:
            recommendations.append(f"⚠️  WARNING: {mismatches} text mismatches detected - check LLM processing")
        
        if not any([missing, extra, chapters_changed, mismatches]):
            recommendations.append("✅ All checks passed - mapping integrity confirmed")
        
        return recommendations


def main():
    """Main entry point for gap detection"""
    try:
        detector = GapDetector()
        report = detector.generate_gap_report()
        
        print("="*60)
        print("GAP DETECTION REPORT")
        print("="*60)
        
        print(f"\nStatus: {report['status']}")
        print(f"Missing UIDs: {report['summary']['missing_uids']}")
        print(f"Extra UIDs: {report['summary']['extra_uids']}")
        print(f"Chapters Changed: {report['summary']['chapters_changed']}")
        print(f"Text Mismatches: {report['summary']['text_mismatches']}")
        
        print(f"\nRecommendations:")
        for rec in report['recommendations']:
            print(f"  {rec}")
        
        # Exit with error code if critical issues found
        if report['status'] == "CRITICAL_GAPS_DETECTED":
            exit(1)
        
    except Exception as e:
        print(f"Gap detection failed: {e}")
        exit(1)


if __name__ == "__main__":
    main() 