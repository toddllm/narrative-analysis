#!/usr/bin/env python3
"""
Ingest script for Zero-Loss Mapping Workflow
Fragments story text into Chapter → Paragraph → Sentence tree with stable UIDs
"""

import json
import re
from pathlib import Path
from typing import List, Dict, Any
import hashlib


class StoryIngestor:
    def __init__(self, story_path: str):
        self.story_path = Path(story_path)
        self.data = []
        self.uid_count = 0
        
    def generate_uid(self, chapter_idx: int, para_idx: int, sent_idx: int) -> str:
        """Generate stable UID for each sentence"""
        return f"CH{chapter_idx:02d}-P{para_idx:03d}-S{sent_idx:03d}"
    
    def calculate_hash(self, text: str) -> str:
        """Calculate hash of text for verification purposes"""
        return hashlib.md5(text.encode()).hexdigest()[:8]
    
    def split_into_sentences(self, text: str) -> List[str]:
        """Split text into sentences, handling various edge cases"""
        # Handle abbreviations and special cases
        text = re.sub(r'\b(Dr|Mr|Mrs|Ms|Prof|Sr|Jr)\.\s*', r'\1<PERIOD> ', text)
        text = re.sub(r'\b(Inc|Ltd|Corp|Co)\.\s*', r'\1<PERIOD> ', text)
        text = re.sub(r'\b(etc|vs|e\.g|i\.e)\.\s*', r'\1<PERIOD> ', text)
        
        # Split on sentence boundaries
        sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
        
        # Restore periods
        sentences = [s.replace('<PERIOD>', '.') for s in sentences]
        
        # Filter out empty sentences
        return [s.strip() for s in sentences if s.strip()]
    
    def clean_text(self, text: str) -> str:
        """Clean text while preserving structure"""
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove leading/trailing whitespace
        return text.strip()
    
    def process_story(self) -> List[Dict[str, Any]]:
        """Process the entire story into structured data"""
        # Read the story file
        story_text = self.story_path.read_text(encoding='utf-8')
        
        # Split into chapters
        # Handle both "Chapter N:" and content without explicit chapter markers
        chapter_splits = re.split(r'^(Chapter \d+:.*?)$', story_text, flags=re.MULTILINE)
        
        # Process chapters
        current_chapter = 0
        for i in range(len(chapter_splits)):
            text = chapter_splits[i].strip()
            if not text:
                continue
                
            # Check if this is a chapter header
            if re.match(r'^Chapter \d+:', text):
                current_chapter += 1
                # Add chapter header as first sentence
                self.data.append({
                    "uid": self.generate_uid(current_chapter, 0, 0),
                    "type": "chapter_header",
                    "chapter": current_chapter,
                    "paragraph": 0,
                    "sentence": 0,
                    "text": self.clean_text(text),
                    "hash": self.calculate_hash(text),
                    "metadata": {
                        "is_header": True,
                        "word_count": len(text.split())
                    }
                })
            else:
                # Process chapter content
                if current_chapter == 0:
                    # Handle content before first chapter marker
                    current_chapter = 1
                
                # Split into paragraphs
                paragraphs = text.split('\n\n')
                
                for para_idx, paragraph in enumerate(paragraphs, 1):
                    paragraph = paragraph.strip()
                    if not paragraph:
                        continue
                    
                    # Check if this is a special paragraph (lists, headers, etc.)
                    is_list = paragraph.startswith(('•', '-', '*', '1.', 'BOSS:', 'THE '))
                    
                    if is_list or ':' in paragraph.split('\n')[0]:
                        # Handle structured content (lists, definitions)
                        lines = paragraph.split('\n')
                        for sent_idx, line in enumerate(lines, 1):
                            line = line.strip()
                            if line:
                                self.uid_count += 1
                                self.data.append({
                                    "uid": self.generate_uid(current_chapter, para_idx, sent_idx),
                                    "type": "structured",
                                    "chapter": current_chapter,
                                    "paragraph": para_idx,
                                    "sentence": sent_idx,
                                    "text": self.clean_text(line),
                                    "hash": self.calculate_hash(line),
                                    "metadata": {
                                        "is_list_item": line.startswith(('•', '-', '*')),
                                        "is_definition": ':' in line,
                                        "word_count": len(line.split())
                                    }
                                })
                    else:
                        # Handle regular paragraphs
                        sentences = self.split_into_sentences(paragraph)
                        for sent_idx, sentence in enumerate(sentences, 1):
                            if sentence:
                                self.uid_count += 1
                                self.data.append({
                                    "uid": self.generate_uid(current_chapter, para_idx, sent_idx),
                                    "type": "sentence",
                                    "chapter": current_chapter,
                                    "paragraph": para_idx,
                                    "sentence": sent_idx,
                                    "text": self.clean_text(sentence),
                                    "hash": self.calculate_hash(sentence),
                                    "metadata": {
                                        "word_count": len(sentence.split()),
                                        "has_dialogue": '"' in sentence or '"' in sentence or '"' in sentence
                                    }
                                })
        
        return self.data
    
    def save_to_json(self, output_path: str):
        """Save processed data to JSON file"""
        output = {
            "metadata": {
                "total_units": self.uid_count,
                "total_chapters": max([d["chapter"] for d in self.data]) if self.data else 0,
                "source_file": str(self.story_path),
                "processing_date": str(Path(output_path).stat().st_mtime if Path(output_path).exists() else "new")
            },
            "data": self.data
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Ingested {self.uid_count} text units")
        print(f"✓ Saved to {output_path}")


def main():
    """Main entry point"""
    # Configure paths
    story_file = "zombie_story.txt"
    output_file = "story.json"
    
    # Process the story
    ingestor = StoryIngestor(story_file)
    ingestor.process_story()
    ingestor.save_to_json(output_file)
    
    # Print summary statistics
    print(f"\nIngestion Summary:")
    print(f"- Total units: {ingestor.uid_count}")
    print(f"- Chapters: {max([d['chapter'] for d in ingestor.data]) if ingestor.data else 0}")
    print(f"- Average words per unit: {sum(d['metadata']['word_count'] for d in ingestor.data) / len(ingestor.data):.1f}")


if __name__ == "__main__":
    main()