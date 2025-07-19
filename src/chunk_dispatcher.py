#!/usr/bin/env python3
"""
Chunk Dispatcher for Zero-Loss Mapping Workflow
Batches sentences into manageable chunks for LLM processing
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime


class ChunkDispatcher:
    def __init__(self, story_json_path: str, batch_size: int = 10):
        """
        Initialize dispatcher with story data and batch size
        
        Args:
            story_json_path: Path to the story.json file
            batch_size: Number of sentences per batch (default 10)
        """
        self.story_json_path = Path(story_json_path)
        self.batch_size = batch_size
        self.story_data = self._load_story_data()
        self.batches = []
        
    def _load_story_data(self) -> Dict[str, Any]:
        """Load the story data from JSON"""
        with open(self.story_json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def create_batches(self) -> List[Dict[str, Any]]:
        """Create batches of sentences for processing"""
        units = self.story_data['data']
        
        # Create batches
        for i in range(0, len(units), self.batch_size):
            batch_units = units[i:i + self.batch_size]
            
            batch = {
                "batch_id": f"BATCH_{i//self.batch_size + 1:04d}",
                "batch_index": i // self.batch_size + 1,
                "total_batches": (len(units) + self.batch_size - 1) // self.batch_size,
                "units_count": len(batch_units),
                "units": batch_units,
                "created_at": datetime.now().isoformat(),
                "status": "pending",
                "prompt": self._generate_prompt(batch_units)
            }
            
            self.batches.append(batch)
        
        return self.batches
    
    def _generate_prompt(self, units: List[Dict[str, Any]]) -> str:
        """Generate the LLM prompt for a batch of units"""
        prompt = """You are the Mapping Agent for a story analysis system.

For each UID below, produce **one Markdown table row** with these columns:
- UID: The unique identifier (copy exactly)
- Raw Sentence: The original text (copy exactly, no changes)
- Narrative Purpose: Brief description of what this text accomplishes in the story
- Characters: Main characters mentioned (comma-separated)
- Locations: Locations mentioned (comma-separated)
- Key Items/Concepts: Important items, abilities, or concepts (comma-separated)
- Links: Related UIDs this connects to (comma-separated, can be empty)

CRITICAL RULES:
1. You MUST include EVERY UID listed below - no omissions
2. Copy the Raw Sentence text EXACTLY as provided
3. Do NOT merge, paraphrase, or skip any entries
4. Each UID gets exactly ONE row
5. Use "N/A" for empty fields rather than leaving blank

Start your response with the table header:
| UID | Raw Sentence | Narrative Purpose | Characters | Locations | Key Items/Concepts | Links |
|-----|--------------|-------------------|------------|-----------|-------------------|-------|

UID List and Sentences:
"""
        
        # Add each unit to the prompt
        for unit in units:
            prompt += f"\n{unit['uid']}: {unit['text']}"
        
        prompt += "\n\nRemember: One row per UID, no omissions, exact text copying."
        
        return prompt
    
    def save_batch(self, batch: Dict[str, Any], output_dir: str = "batches"):
        """Save a single batch to file"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        batch_file = output_path / f"{batch['batch_id']}.json"
        with open(batch_file, 'w', encoding='utf-8') as f:
            json.dump(batch, f, indent=2, ensure_ascii=False)
    
    def save_all_batches(self, output_dir: str = "batches"):
        """Save all batches to files"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Save individual batch files
        for batch in self.batches:
            self.save_batch(batch, output_dir)
        
        # Save batch manifest
        manifest = {
            "total_batches": len(self.batches),
            "batch_size": self.batch_size,
            "total_units": len(self.story_data['data']),
            "created_at": datetime.now().isoformat(),
            "batches": [
                {
                    "batch_id": batch["batch_id"],
                    "units_count": batch["units_count"],
                    "status": batch["status"]
                }
                for batch in self.batches
            ]
        }
        
        manifest_file = output_path / "manifest.json"
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)
        
        print(f"✓ Created {len(self.batches)} batches")
        print(f"✓ Saved to {output_dir}/")
    
    def get_batch_prompt(self, batch_id: str) -> str:
        """Get the prompt for a specific batch"""
        for batch in self.batches:
            if batch["batch_id"] == batch_id:
                return batch["prompt"]
        raise ValueError(f"Batch {batch_id} not found")
    
    def estimate_processing_stats(self):
        """Estimate processing statistics"""
        total_units = len(self.story_data['data'])
        total_batches = len(self.batches)
        avg_words_per_unit = sum(u['metadata']['word_count'] for u in self.story_data['data']) / total_units
        
        print(f"\nProcessing Statistics:")
        print(f"- Total units to process: {total_units}")
        print(f"- Batch size: {self.batch_size} units")
        print(f"- Total batches: {total_batches}")
        print(f"- Average words per unit: {avg_words_per_unit:.1f}")
        print(f"- Estimated tokens per batch: ~{self.batch_size * avg_words_per_unit * 1.5:.0f}")


def main():
    """Main entry point"""
    # Configure
    story_file = "story.json"
    batch_size = 15  # Optimal size for LLM context
    output_dir = "batches"
    
    # Create dispatcher
    dispatcher = ChunkDispatcher(story_file, batch_size)
    
    # Create and save batches
    dispatcher.create_batches()
    dispatcher.save_all_batches(output_dir)
    
    # Show statistics
    dispatcher.estimate_processing_stats()
    
    # Show sample prompt for first batch
    print(f"\nSample prompt for first batch:")
    print("-" * 50)
    sample_prompt = dispatcher.get_batch_prompt("BATCH_0001")
    print(sample_prompt[:500] + "..." if len(sample_prompt) > 500 else sample_prompt)


if __name__ == "__main__":
    main()