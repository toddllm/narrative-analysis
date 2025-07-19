#!/usr/bin/env python3
"""
Merge Script for Zero-Loss Mapping Workflow
Aggregates verified chunks into master mapping file
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import csv


class ChunkMerger:
    def __init__(self, results_dir: str = "results", story_json: str = "story.json"):
        """
        Initialize merger with results directory
        
        Args:
            results_dir: Directory containing processed batch results
            story_json: Path to original story.json for reference
        """
        self.results_dir = Path(results_dir)
        self.story_json = Path(story_json)
        self.story_data = self._load_story_data()
        self.merged_data = []
        self.merge_stats = {
            "total_units": 0,
            "batches_processed": 0,
            "errors": [],
            "warnings": []
        }
        
    def _load_story_data(self) -> Dict[str, Any]:
        """Load original story data for reference"""
        with open(self.story_json, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def load_batch_result(self, result_file: Path) -> Optional[List[Dict[str, str]]]:
        """Load a single batch result file"""
        try:
            with open(result_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Check if this result was verified and accepted
            if data.get('verification', {}).get('recommendation', '').startswith(('ACCEPT', 'ACCEPT_WITH_WARNINGS')):
                return data.get('parsed_rows', [])
            else:
                self.merge_stats['warnings'].append(f"Skipped {result_file.name}: {data.get('verification', {}).get('recommendation', 'No verification')}")
                return None
                
        except Exception as e:
            self.merge_stats['errors'].append(f"Error loading {result_file.name}: {str(e)}")
            return None
    
    def merge_all_results(self):
        """Merge all batch results into unified dataset"""
        # Get all result files sorted by batch number
        result_files = sorted(self.results_dir.glob("BATCH_*.json"))
        
        if not result_files:
            raise ValueError(f"No batch result files found in {self.results_dir}")
        
        # Process each batch
        for result_file in result_files:
            batch_rows = self.load_batch_result(result_file)
            
            if batch_rows:
                self.merged_data.extend(batch_rows)
                self.merge_stats['batches_processed'] += 1
                self.merge_stats['total_units'] += len(batch_rows)
        
        # Sort by UID to maintain order
        self.merged_data.sort(key=lambda x: x.get('UID', ''))
    
    def enrich_with_metadata(self):
        """Add metadata from original story to merged data"""
        # Create lookup for original data
        uid_to_meta = {unit['uid']: unit for unit in self.story_data['data']}
        
        for row in self.merged_data:
            uid = row.get('UID', '')
            if uid in uid_to_meta:
                meta = uid_to_meta[uid]
                row['chapter'] = meta.get('chapter', 0)
                row['paragraph'] = meta.get('paragraph', 0)
                row['sentence'] = meta.get('sentence', 0)
                row['type'] = meta.get('type', 'unknown')
                row['word_count'] = meta.get('metadata', {}).get('word_count', 0)
    
    def generate_markdown_mapping(self) -> str:
        """Generate the master mapping in Markdown format"""
        markdown = "# Zombie Infection Chaos - Complete Story Mapping\n\n"
        markdown += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        markdown += f"Total Units: {len(self.merged_data)}\n\n"
        
        # Group by chapter
        current_chapter = None
        
        markdown += "## Table of Contents\n\n"
        chapters = sorted(set(row.get('chapter', 0) for row in self.merged_data))
        for ch in chapters:
            if ch > 0:
                ch_rows = [r for r in self.merged_data if r.get('chapter', 0) == ch]
                ch_title = next((r['Raw Sentence'] for r in ch_rows if r.get('type') == 'chapter_header'), f"Chapter {ch}")
                markdown += f"- [{ch_title}](#chapter-{ch})\n"
        
        markdown += "\n---\n\n"
        
        # Main content table
        for row in self.merged_data:
            chapter = row.get('chapter', 0)
            
            # Add chapter header
            if chapter != current_chapter:
                current_chapter = chapter
                if chapter > 0:
                    markdown += f"\n## Chapter {chapter}\n\n"
                    # Find chapter title
                    ch_title = next((r['Raw Sentence'] for r in self.merged_data 
                                   if r.get('chapter') == chapter and r.get('type') == 'chapter_header'), '')
                    if ch_title:
                        markdown += f"**{ch_title}**\n\n"
                    
                # Table header for this chapter
                markdown += "| UID | Text | Purpose | Characters | Locations | Key Items | Links |\n"
                markdown += "|-----|------|---------|------------|-----------|-----------|-------|\n"
            
            # Add row
            uid = row.get('UID', '')
            text = row.get('Raw Sentence', '').replace('|', '\\|')  # Escape pipes
            purpose = row.get('Narrative Purpose', '').replace('|', '\\|')
            characters = row.get('Characters', 'N/A').replace('|', '\\|')
            locations = row.get('Locations', 'N/A').replace('|', '\\|')
            items = row.get('Key Items/Concepts', 'N/A').replace('|', '\\|')
            links = row.get('Links', 'N/A').replace('|', '\\|')
            
            # Truncate very long text for readability
            if len(text) > 150:
                text = text[:147] + "..."
            
            markdown += f"| {uid} | {text} | {purpose} | {characters} | {locations} | {items} | {links} |\n"
        
        return markdown
    
    def generate_csv_mapping(self) -> str:
        """Generate the master mapping in CSV format"""
        output = []
        
        # Headers
        headers = ['UID', 'Chapter', 'Paragraph', 'Sentence', 'Type', 'Raw Sentence', 
                  'Narrative Purpose', 'Characters', 'Locations', 'Key Items/Concepts', 
                  'Links', 'Word Count']
        
        # Rows
        for row in self.merged_data:
            csv_row = [
                row.get('UID', ''),
                str(row.get('chapter', 0)),
                str(row.get('paragraph', 0)),
                str(row.get('sentence', 0)),
                row.get('type', ''),
                row.get('Raw Sentence', ''),
                row.get('Narrative Purpose', ''),
                row.get('Characters', 'N/A'),
                row.get('Locations', 'N/A'),
                row.get('Key Items/Concepts', 'N/A'),
                row.get('Links', 'N/A'),
                str(row.get('word_count', 0))
            ]
            output.append(csv_row)
        
        return headers, output
    
    def save_mappings(self, output_prefix: str = "mapping"):
        """Save mappings in multiple formats"""
        # Save as Markdown
        markdown_content = self.generate_markdown_mapping()
        markdown_file = f"{output_prefix}.md"
        with open(markdown_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        print(f"✓ Saved Markdown mapping to {markdown_file}")
        
        # Save as CSV
        headers, csv_data = self.generate_csv_mapping()
        csv_file = f"{output_prefix}.csv"
        with open(csv_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(csv_data)
        print(f"✓ Saved CSV mapping to {csv_file}")
        
        # Save as JSON (structured data)
        json_file = f"{output_prefix}.json"
        json_data = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_units": len(self.merged_data),
                "batches_processed": self.merge_stats['batches_processed'],
                "source_file": str(self.story_json)
            },
            "mapping": self.merged_data,
            "statistics": self.generate_statistics()
        }
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved JSON mapping to {json_file}")
    
    def generate_statistics(self) -> Dict[str, Any]:
        """Generate mapping statistics"""
        # Character frequency
        all_characters = []
        for row in self.merged_data:
            chars = row.get('Characters', '')
            if chars and chars != 'N/A':
                all_characters.extend([c.strip() for c in chars.split(',')])
        
        char_freq = {}
        for char in all_characters:
            char_freq[char] = char_freq.get(char, 0) + 1
        
        # Location frequency
        all_locations = []
        for row in self.merged_data:
            locs = row.get('Locations', '')
            if locs and locs != 'N/A':
                all_locations.extend([l.strip() for l in locs.split(',')])
        
        loc_freq = {}
        for loc in all_locations:
            loc_freq[loc] = loc_freq.get(loc, 0) + 1
        
        # Key items/concepts
        all_items = []
        for row in self.merged_data:
            items = row.get('Key Items/Concepts', '')
            if items and items != 'N/A':
                all_items.extend([i.strip() for i in items.split(',')])
        
        item_freq = {}
        for item in all_items:
            item_freq[item] = item_freq.get(item, 0) + 1
        
        return {
            "total_chapters": len(set(row.get('chapter', 0) for row in self.merged_data)),
            "total_units": len(self.merged_data),
            "units_by_type": {
                t: len([r for r in self.merged_data if r.get('type') == t])
                for t in set(row.get('type', 'unknown') for row in self.merged_data)
            },
            "top_characters": sorted(char_freq.items(), key=lambda x: x[1], reverse=True)[:10],
            "top_locations": sorted(loc_freq.items(), key=lambda x: x[1], reverse=True)[:10],
            "top_items": sorted(item_freq.items(), key=lambda x: x[1], reverse=True)[:10],
            "total_word_count": sum(row.get('word_count', 0) for row in self.merged_data)
        }
    
    def print_summary(self):
        """Print merge summary"""
        stats = self.generate_statistics()
        
        print(f"\n{'='*60}")
        print("MERGE SUMMARY")
        print(f"{'='*60}")
        print(f"Total units merged: {self.merge_stats['total_units']}")
        print(f"Batches processed: {self.merge_stats['batches_processed']}")
        print(f"Total chapters: {stats['total_chapters']}")
        print(f"Total word count: {stats['total_word_count']:,}")
        
        if self.merge_stats['errors']:
            print(f"\nErrors: {len(self.merge_stats['errors'])}")
            for error in self.merge_stats['errors'][:5]:
                print(f"  - {error}")
        
        if self.merge_stats['warnings']:
            print(f"\nWarnings: {len(self.merge_stats['warnings'])}")
            for warning in self.merge_stats['warnings'][:5]:
                print(f"  - {warning}")
        
        print(f"\nTop Characters:")
        for char, count in stats['top_characters'][:5]:
            print(f"  - {char}: {count} mentions")
        
        print(f"\nTop Locations:")
        for loc, count in stats['top_locations'][:5]:
            print(f"  - {loc}: {count} mentions")
        
        print(f"{'='*60}\n")


def main():
    """Main entry point for testing"""
    print("Merger module ready.")
    print("This will be called by the orchestrator after all batches are processed.")
    print("\nTo test with sample data, run the orchestrator.py")


if __name__ == "__main__":
    main()