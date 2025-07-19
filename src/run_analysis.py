#!/usr/bin/env python3
"""
Narrative Analysis Runner
A comprehensive CLI script for running narrative analysis with progress tracking
"""

import json
import os
import sys
import time
import argparse
import signal
from pathlib import Path
from datetime import datetime
from colorama import init, Fore, Back, Style

# Initialize colorama for cross-platform colored output
init()

# Import our modules
from orchestrator import MappingOrchestrator
from ingest import StoryIngestor
from chunk_dispatcher import ChunkDispatcher
from verifier import MappingVerifier
from merge_chunks import ChunkMerger
from post_processor import PostProcessor
from gap_detector import GapDetector

class ProgressTracker:
    """Track and display progress with optional verbose output"""
    
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.start_time = None
        self.current_step = 0
        self.total_steps = 6
        self.substep_progress = 0
        self.substep_total = 0
        self.cancelled = False
        
    def start(self):
        """Start tracking progress"""
        self.start_time = datetime.now()
        self.print_header()
        
    def print_header(self):
        """Print analysis header"""
        print(f"\n{Fore.CYAN}{'='*60}")
        print(f"{Fore.CYAN}🔬 NARRATIVE ANALYSIS PIPELINE")
        print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}\n")
        
    def step(self, step_num, description):
        """Mark the start of a new step"""
        self.current_step = step_num
        elapsed = self.get_elapsed_time()
        print(f"\n{Fore.YELLOW}[{elapsed}] Step {step_num}/{self.total_steps}: {description}{Style.RESET_ALL}")
        
    def substep_init(self, total):
        """Initialize substep progress tracking"""
        self.substep_progress = 0
        self.substep_total = total
        
    def substep_update(self, current, message=""):
        """Update substep progress"""
        self.substep_progress = current
        if self.verbose or current % 10 == 0 or current == self.substep_total:
            self.print_progress_bar(current, self.substep_total, message)
            
    def print_progress_bar(self, current, total, message=""):
        """Print a progress bar"""
        if total == 0:
            return
            
        percent = (current / total) * 100
        bar_length = 40
        filled_length = int(bar_length * current // total)
        
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        # Clear line and print progress
        print(f'\r{Fore.GREEN}Progress: [{bar}] {percent:.1f}% ({current}/{total}) {message}{Style.RESET_ALL}', end='', flush=True)
        
        if current == total:
            print()  # New line when complete
            
    def success(self, message):
        """Print success message"""
        print(f"{Fore.GREEN}✓ {message}{Style.RESET_ALL}")
        
    def warning(self, message):
        """Print warning message"""
        print(f"{Fore.YELLOW}⚠ {message}{Style.RESET_ALL}")
        
    def error(self, message):
        """Print error message"""
        print(f"{Fore.RED}✗ {message}{Style.RESET_ALL}")
        
    def info(self, message):
        """Print info message (only in verbose mode)"""
        if self.verbose:
            print(f"{Fore.BLUE}ℹ {message}{Style.RESET_ALL}")
            
    def debug(self, message):
        """Print debug message (only in verbose mode)"""
        if self.verbose:
            print(f"{Fore.MAGENTA}🔍 {message}{Style.RESET_ALL}")
            
    def get_elapsed_time(self):
        """Get elapsed time as formatted string"""
        if not self.start_time:
            return "00:00"
        elapsed = datetime.now() - self.start_time
        minutes = int(elapsed.total_seconds() / 60)
        seconds = int(elapsed.total_seconds() % 60)
        return f"{minutes:02d}:{seconds:02d}"
        
    def complete(self):
        """Mark analysis as complete"""
        elapsed = self.get_elapsed_time()
        print(f"\n{Fore.CYAN}{'='*60}")
        print(f"{Fore.GREEN}✓ Analysis completed successfully in {elapsed}")
        print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}\n")
        
    def cancel(self):
        """Mark analysis as cancelled"""
        self.cancelled = True
        elapsed = self.get_elapsed_time()
        print(f"\n{Fore.YELLOW}{'='*60}")
        print(f"{Fore.YELLOW}⚠ Analysis cancelled by user at {elapsed}")
        print(f"{Fore.YELLOW}{'='*60}{Style.RESET_ALL}\n")

class NarrativeAnalyzer:
    """Main analyzer class"""
    
    def __init__(self, story_file, model_name="qwen2.5:32b", batch_size=10, 
                 use_mock=False, verbose=False):
        self.story_file = story_file
        self.model_name = model_name
        self.batch_size = batch_size
        self.use_mock = use_mock
        self.progress = ProgressTracker(verbose)
        self.cancelled = False
        
        # Register signal handler for Ctrl+C
        signal.signal(signal.SIGINT, self._signal_handler)
        
    def _signal_handler(self, sig, frame):
        """Handle Ctrl+C gracefully"""
        self.cancelled = True
        self.progress.cancel()
        sys.exit(0)
        
    def run(self):
        """Run the complete analysis pipeline"""
        self.progress.start()
        
        try:
            # Step 1: Ingest story
            self.progress.step(1, "Ingesting story")
            story_data = self._ingest_story()
            
            if self.cancelled:
                return
                
            # Step 2: Create batches
            self.progress.step(2, "Creating batches")
            batches = self._create_batches()
            
            if self.cancelled:
                return
                
            # Step 3: Process with LLM
            self.progress.step(3, f"Processing {len(batches)} batches with {self.model_name}")
            self._process_batches(batches)
            
            if self.cancelled:
                return
                
            # Step 4: Merge results
            self.progress.step(4, "Merging results")
            stats = self._merge_results()
            
            if self.cancelled:
                return
                
            # Step 5: Generate visualizations
            self.progress.step(5, "Generating visualizations and reports")
            self._generate_outputs()
            
            if self.cancelled:
                return
                
            # Step 6: Verify integrity
            self.progress.step(6, "Verifying data integrity")
            self._verify_integrity()
            
            # Complete!
            self.progress.complete()
            self._print_summary()
            
        except Exception as e:
            self.progress.error(f"Fatal error: {str(e)}")
            raise
            
    def _ingest_story(self):
        """Ingest the story file"""
        if Path("story.json").exists():
            with open("story.json", 'r') as f:
                story_data = json.load(f)
            
            # Check if it's from the same file
            existing_source = story_data.get("metadata", {}).get("source_file", "unknown")
            current_file = os.path.basename(self.story_file)
            
            if existing_source != current_file:
                self.progress.warning(f"Found cached data from different file: {existing_source}")
                self.progress.warning(f"Current file: {current_file}")
                self.progress.warning("Use --clean flag to start fresh: python run_analysis.py <file> --clean")
                self.progress.info("Loading cached data anyway...")
            else:
                self.progress.info("Found existing story.json, loading...")
                
            self.progress.success(f"Loaded {len(story_data['data'])} text units")
            return story_data
        else:
            self.progress.info(f"Processing {self.story_file}...")
            ingestor = StoryIngestor(self.story_file)
            ingestor.process_story()
            ingestor.save_to_json("story.json")
            self.progress.success(f"Ingested {len(ingestor.data)} text units")
            # Return in same format as loaded JSON
            return {"data": ingestor.data}
            
    def _create_batches(self):
        """Create processing batches"""
        dispatcher = ChunkDispatcher("story.json", self.batch_size)
        batches = dispatcher.create_batches()
        dispatcher.save_all_batches("batches")
        
        self.progress.success(f"Created {len(batches)} batches of {self.batch_size} units each")
        
        # Show estimated processing time
        if not self.use_mock:
            # Estimate ~10-20 seconds per batch for real LLM
            estimated_minutes = (len(batches) * 15) / 60
            self.progress.info(f"Estimated processing time: {estimated_minutes:.1f} minutes")
            
        return batches
        
    def _process_batches(self, batches):
        """Process batches with LLM"""
        orchestrator = MappingOrchestrator(
            story_file=self.story_file,
            batch_size=self.batch_size,
            use_mock_llm=self.use_mock,
            model_name=self.model_name
        )
        
        self.progress.substep_init(len(batches))
        
        success_count = 0
        failed_count = 0
        
        for i, batch in enumerate(batches):
            if self.cancelled:
                return
                
            batch_id = batch['batch_id']
            self.progress.debug(f"Processing {batch_id}...")
            
            result = orchestrator.process_batch(batch)
            
            if result:
                success_count += 1
                self.progress.debug(f"✓ {batch_id} processed successfully")
            else:
                failed_count += 1
                self.progress.warning(f"Failed to process {batch_id}")
                
            self.progress.substep_update(i + 1, f"{batch_id}")
            
        self.progress.success(f"Processed {success_count} batches successfully")
        if failed_count > 0:
            self.progress.warning(f"{failed_count} batches failed processing")
            
    def _merge_results(self):
        """Merge all batch results"""
        self.progress.info("Loading and merging batch results...")
        
        merger = ChunkMerger("results")
        merger.merge_all_results()
        merger.enrich_with_metadata()
        
        # Save outputs
        merger.save_mappings("mapping")
        
        stats = merger.merge_stats
        self.progress.success(f"Merged {stats['total_units']} units from {stats['batches_processed']} batches")
        
        return stats
        
    def _generate_outputs(self):
        """Generate visualizations and derived views"""
        self.progress.info("Creating visualizations...")
        
        processor = PostProcessor("mapping.json")
        processor.save_all_views("derived_views")
        
        self.progress.success("Generated character atlas, location gazetteer, and visualizations")
            
    def _verify_integrity(self):
        """Verify data integrity and check for gaps"""
        detector = GapDetector("story.json", "mapping.json")
        missing_uids = detector.detect_missing_uids()
        
        if missing_uids:
            self.progress.warning(f"Found {len(missing_uids)} missing UIDs")
            if self.progress.verbose:
                # Convert set to list for slicing
                missing_list = list(missing_uids)
                self.progress.info(f"Missing UIDs: {missing_list[:10]}...")
        else:
            self.progress.success("100% coverage achieved - no gaps detected!")
            
        # Generate gap report
        gap_report = detector.generate_gap_report()
        with open("gap_report.json", 'w', encoding='utf-8') as f:
            json.dump(gap_report, f, indent=2)
        
    def _print_summary(self):
        """Print analysis summary"""
        print(f"\n{Fore.CYAN}📊 Analysis Summary:{Style.RESET_ALL}")
        print(f"{'─'*40}")
        
        # List generated files
        files = [
            ("mapping.json", "Structured narrative data"),
            ("mapping.csv", "Spreadsheet format"),
            ("mapping.md", "Human-readable report"),
            ("derived_views/character_network.png", "Character relationship graph"),
            ("derived_views/location_flow.png", "Location connections"),
            ("derived_views/narrative_flow.json", "Story progression data"),
        ]
        
        print(f"\n{Fore.GREEN}Generated Files:{Style.RESET_ALL}")
        for file_path, description in files:
            if Path(file_path).exists():
                size = Path(file_path).stat().st_size / 1024  # KB
                print(f"  • {file_path:<35} ({size:>6.1f} KB) - {description}")
                
        print(f"\n{Fore.CYAN}Next Steps:{Style.RESET_ALL}")
        print("  1. Review mapping.md for the complete narrative analysis")
        print("  2. Open character_network.png to see character relationships")
        print("  3. Use mapping.csv for data analysis in spreadsheet software")
        print("  4. Run gap_detector.py if you need to identify missing sections")

def main():
    parser = argparse.ArgumentParser(
        description='Run narrative analysis on a story file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default settings
  python run_analysis.py story.txt
  
  # Use mock LLM for testing
  python run_analysis.py story.txt --mock
  
  # Use specific model with custom batch size
  python run_analysis.py story.txt --model llama3.1:8b --batch-size 5
  
  # Enable verbose output
  python run_analysis.py story.txt --verbose
        """
    )
    
    parser.add_argument('story_file', help='Path to the story text file')
    parser.add_argument('--model', dest='model_name', default='qwen2.5:32b',
                        help='Ollama model to use (default: qwen2.5:32b)')
    parser.add_argument('--batch-size', type=int, default=10,
                        help='Number of sentences per batch (default: 10)')
    parser.add_argument('--mock', action='store_true',
                        help='Use mock LLM for testing (fast but basic)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose output')
    parser.add_argument('--clean', action='store_true',
                        help='Clean all cached data before running')
    
    args = parser.parse_args()
    
    # Clean cached data if requested
    if args.clean:
        print(f"{Fore.YELLOW}Cleaning cached data...{Style.RESET_ALL}")
        import shutil
        paths_to_clean = ['story.json', 'batches/', 'results/', 'output/', 
                         'mapping.md', 'mapping.csv', 'mapping.json', 
                         'derived_views/', 'gap_report.json']
        for path in paths_to_clean:
            if Path(path).exists():
                if Path(path).is_dir():
                    shutil.rmtree(path)
                else:
                    Path(path).unlink()
                print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Removed {path}")
        print()
    
    # Verify story file exists
    if not Path(args.story_file).exists():
        print(f"{Fore.RED}Error: Story file '{args.story_file}' not found{Style.RESET_ALL}")
        sys.exit(1)
        
    # Run analysis
    analyzer = NarrativeAnalyzer(
        story_file=args.story_file,
        model_name=args.model_name,
        batch_size=args.batch_size,
        use_mock=args.mock,
        verbose=args.verbose
    )
    
    analyzer.run()

if __name__ == '__main__':
    main()