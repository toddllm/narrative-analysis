#!/usr/bin/env python3
"""
Orchestrator for Zero-Loss Mapping Workflow
Coordinates the entire pipeline from ingestion to final mapping
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import subprocess
import argparse
import ollama

# Import our modules
from ingest import StoryIngestor
from chunk_dispatcher import ChunkDispatcher
from verifier import MappingVerifier
from merge_chunks import ChunkMerger


class MappingOrchestrator:
    def __init__(self, 
                 story_file: str = "zombie_story.txt",
                 batch_size: int = 15,
                 use_mock_llm: bool = False,
                 model_name: str = "qwen2.5:72b"):
        """
        Initialize the orchestrator
        
        Args:
            story_file: Path to the story text file
            batch_size: Number of sentences per batch
            use_mock_llm: Use mock LLM for testing (False by default)
            model_name: Ollama model to use for analysis
        """
        self.story_file = story_file
        self.batch_size = batch_size
        self.use_mock_llm = use_mock_llm
        self.model_name = model_name
        self.results_dir = Path("results")
        self.results_dir.mkdir(exist_ok=True)
        
        self.stats = {
            "start_time": datetime.now(),
            "batches_total": 0,
            "batches_processed": 0,
            "batches_failed": 0,
            "total_units": 0,
            "units_verified": 0
        }
    
    def real_llm_process(self, batch: Dict[str, Any]) -> str:
        """Process batch using real LLM (Ollama)"""
        # Create the prompt
        prompt = batch['prompt']
        
        # Add specific instructions for better analysis
        enhanced_prompt = f"""You are an expert literary analyst. {prompt}

CRITICAL ANALYSIS REQUIREMENTS:
- Characters: Only extract actual character names (people, beings, entities with names)
- Locations: Only extract specific place names, buildings, geographic locations
- Key Items/Concepts: Extract important objects, abilities, technologies, abstract concepts
- Narrative Purpose: Be concise but specific about the story function
- Links: Reference UIDs that are thematically or narratively connected

EXAMPLES OF GOOD EXTRACTION:
- Characters: "Jake", "Maya", "Zeldina", "Dr. Sarah" (NOT "Steam", "Chapter", "Complete")
- Locations: "Nexus Prime Factory", "Sewers", "Crystal Falls" (NOT random words)
- Key Items: "Reality Orbs", "Frostbane Cannon", "The Cube", "Portal"

Be accurate and only extract meaningful story elements."""

        try:
            # Call Ollama
            response = ollama.chat(
                model=self.model_name,
                messages=[
                    {
                        'role': 'user',
                        'content': enhanced_prompt
                    }
                ],
                options={
                    'temperature': 0.1,  # Low temperature for consistent extraction
                    'top_p': 0.9,
                    'repeat_penalty': 1.1
                }
            )
            
            return response['message']['content']
            
        except Exception as e:
            print(f"Error calling Ollama: {e}")
            # Fallback to mock if LLM fails
            return self.mock_llm_process(batch)
    
    def mock_llm_process(self, batch: Dict[str, Any]) -> str:
        """Mock LLM processor for testing"""
        # Generate a realistic-looking response
        response = "| UID | Raw Sentence | Narrative Purpose | Characters | Locations | Key Items/Concepts | Links |\n"
        response += "|-----|--------------|-------------------|------------|-----------|--------------------|---------|\n"
        
        for unit in batch['units']:
            uid = unit['uid']
            text = unit['text'].replace('|', '\\|')
            
            # Simple analysis
            purpose = "Establishes setting" if unit['paragraph'] == 1 else "Develops narrative"
            
            # Extract characters (simple name detection)
            characters = []
            for word in unit['text'].split():
                if word[0].isupper() and len(word) > 2 and word not in ['The', 'This', 'That', 'These']:
                    characters.append(word.strip('.,!?'))
            chars = ', '.join(set(characters)) if characters else 'N/A'
            
            # Extract locations
            locations = []
            if 'factory' in unit['text'].lower():
                locations.append('Factory')
            if 'city' in unit['text'].lower():
                locations.append('City')
            locs = ', '.join(locations) if locations else 'N/A'
            
            # Items/concepts
            items = []
            keywords = ['zombie', 'orb', 'void', 'crystal', 'weapon', 'shield']
            for kw in keywords:
                if kw in unit['text'].lower():
                    items.append(kw.capitalize())
            items_str = ', '.join(items) if items else 'N/A'
            
            # Links (simple - link to next UID if sequential)
            links = 'N/A'
            
            response += f"| {uid} | {text} | {purpose} | {chars} | {locs} | {items_str} | {links} |\n"
        
        return response

    def process_batch(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single batch through LLM and verification"""
        batch_id = batch['batch_id']
        print(f"\nProcessing {batch_id}...")
        
        try:
            # Get LLM response
            if self.use_mock_llm:
                llm_response = self.mock_llm_process(batch)
            else:
                llm_response = self.real_llm_process(batch)
            
            # Verify the response
            batch_file = Path("batches") / f"{batch_id}.json"
            verifier = MappingVerifier(str(batch_file))
            parsed_rows = verifier.parse_markdown_table(llm_response)
            verification_report = verifier.generate_report(parsed_rows, llm_response)
            
            # Save result
            result = {
                "batch_id": batch_id,
                "processed_at": datetime.now().isoformat(),
                "llm_response": llm_response,
                "parsed_rows": parsed_rows,
                "verification": verification_report
            }
            
            result_file = self.results_dir / f"{batch_id}.json"
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            # Update stats
            if verification_report['recommendation'].startswith('ACCEPT'):
                self.stats['units_verified'] += len(parsed_rows)
                print(f"✓ {batch_id} processed successfully ({len(parsed_rows)} units)")
            else:
                self.stats['batches_failed'] += 1
                print(f"✗ {batch_id} failed verification: {verification_report['recommendation']}")
            
            return result
            
        except Exception as e:
            print(f"✗ Error processing {batch_id}: {str(e)}")
            self.stats['batches_failed'] += 1
            return None
    
    def run_pipeline(self):
        """Run the complete pipeline"""
        print("="*60)
        print("ZERO-LOSS MAPPING PIPELINE")
        print("="*60)
        
        # Step 1: Ingest
        print("\n[1/5] Ingesting story...")
        if not Path("story.json").exists():
            ingestor = StoryIngestor(self.story_file)
            ingestor.process_story()
            ingestor.save_to_json("story.json")
        else:
            print("✓ Using existing story.json")
        
        # Step 2: Create batches
        print("\n[2/5] Creating batches...")
        dispatcher = ChunkDispatcher("story.json", self.batch_size)
        batches = dispatcher.create_batches()
        dispatcher.save_all_batches("batches")
        
        self.stats['batches_total'] = len(batches)
        self.stats['total_units'] = len(dispatcher.story_data['data'])
        
        # Step 3: Process batches
        print(f"\n[3/5] Processing {len(batches)} batches...")
        progress_bar_width = 50
        
        for i, batch in enumerate(batches):
            # Update progress
            progress = (i + 1) / len(batches)
            filled = int(progress_bar_width * progress)
            bar = "█" * filled + "░" * (progress_bar_width - filled)
            print(f"\rProgress: [{bar}] {i+1}/{len(batches)}", end="", flush=True)
            
            # Process batch
            self.process_batch(batch)
            self.stats['batches_processed'] += 1
            
            # Small delay to simulate API rate limiting
            if not self.use_mock_llm:
                time.sleep(0.5)
        
        print()  # New line after progress bar
        
        # Step 4: Merge results
        print("\n[4/5] Merging results...")
        merger = ChunkMerger("results", "story.json")
        merger.merge_all_results()
        merger.enrich_with_metadata()
        merger.save_mappings("mapping")
        merger.print_summary()
        
        # Step 5: Final report
        print("\n[5/5] Pipeline complete!")
        self.print_final_report()
    
    def print_final_report(self):
        """Print final pipeline report"""
        duration = (datetime.now() - self.stats['start_time']).total_seconds()
        
        print("\n" + "="*60)
        print("PIPELINE SUMMARY")
        print("="*60)
        print(f"Total time: {duration:.1f} seconds")
        print(f"Total units: {self.stats['total_units']}")
        print(f"Units verified: {self.stats['units_verified']}")
        print(f"Verification rate: {self.stats['units_verified']/self.stats['total_units']*100:.1f}%")
        print(f"Batches processed: {self.stats['batches_processed']}/{self.stats['batches_total']}")
        print(f"Batches failed: {self.stats['batches_failed']}")
        print("\nOutput files:")
        print("  - mapping.md (Markdown format)")
        print("  - mapping.csv (Spreadsheet format)")
        print("  - mapping.json (Structured data)")
        print("="*60)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Zero-Loss Story Mapping Pipeline")
    parser.add_argument("--story", default="../examples/sample_story.txt", help="Path to story file")
    parser.add_argument("--batch-size", type=int, default=15, help="Sentences per batch")
    parser.add_argument("--mock-llm", action="store_true", help="Use mock LLM instead of real Ollama model")
    parser.add_argument("--model", default="qwen2.5:72b", help="Ollama model to use")
    
    args = parser.parse_args()
    
    # Check if story file exists
    if not Path(args.story).exists():
        print(f"Error: Story file '{args.story}' not found!")
        sys.exit(1)
    
    # Run the pipeline
    orchestrator = MappingOrchestrator(
        story_file=args.story,
        batch_size=args.batch_size,
        use_mock_llm=args.mock_llm,
        model_name=args.model
    )
    
    try:
        orchestrator.run_pipeline()
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nPipeline error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()