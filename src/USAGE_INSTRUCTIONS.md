# Narrative Analysis Pipeline - Clear Usage Instructions

## Important: Clean State Before Running

The pipeline caches data to avoid re-processing. **ALWAYS clean up before analyzing a new story**:

```bash
# Clean all cached data and outputs
rm -rf story.json batches/ results/ output/ mapping* derived_views/ gap_report.json
```

## Step-by-Step Instructions

### 1. Navigate to the src directory
```bash
cd ~/monprime-work/narrative-analysis/src
```

### 2. Clean any previous analysis data
```bash
rm -rf story.json batches/ results/ output/ mapping* derived_views/ gap_report.json
```

### 3. Run the analysis

#### For zeldina-story.txt (full analysis):
```bash
# Basic run (will take 30-45 minutes)
python run_analysis.py ~/monprime-work/zeldina-story.txt

# With progress updates
python run_analysis.py ~/monprime-work/zeldina-story.txt --verbose

# With custom batch size (larger = more efficient but uses more memory)
python run_analysis.py ~/monprime-work/zeldina-story.txt --batch-size 20 --verbose
```

#### For testing with smaller files:
```bash
# Test with first 100 lines
python run_analysis.py test_story.txt --verbose

# Test with mock LLM (no real processing)
python run_analysis.py test_story.txt --mock
```

## Understanding the Output

After analysis completes, you'll have:

1. **mapping.md** - Human-readable report with character interactions and story flow
2. **mapping.csv** - Spreadsheet format for data analysis
3. **mapping.json** - Structured data for programming
4. **derived_views/character_network.png** - Visual character relationship graph
5. **derived_views/summary_report.md** - High-level summary

## Common Issues and Solutions

### "Found existing story.json, loading..."
This means you have cached data from a previous run. Clean it up:
```bash
rm -rf story.json batches/ results/
```

### "Error: Story file not found"
Use the full path or relative path from the src directory:
```bash
# Full path (recommended)
python run_analysis.py ~/monprime-work/zeldina-story.txt

# Or relative path from src directory
python run_analysis.py ../../zeldina-story.txt
```

### Processing seems stuck
The LLM takes 10-15 seconds per batch. For zeldina-story.txt:
- ~3,000 text units ÷ 20 units/batch = ~150 batches
- 150 batches × 15 seconds = ~38 minutes total

Use `--verbose` to see progress.

## Web Interface Alternative

For real-time monitoring with a web interface:

```bash
# Start the server
python analysis_server.py

# Open browser to http://localhost:5000
# Upload file or enter path: /home/tdeshane/monprime-work/zeldina-story.txt
```

## Full Example Session

```bash
# 1. Go to source directory
cd ~/monprime-work/narrative-analysis/src

# 2. Clean any previous data
rm -rf story.json batches/ results/ output/ mapping* derived_views/ gap_report.json

# 3. Run the full analysis with progress
python run_analysis.py ~/monprime-work/zeldina-story.txt --batch-size 20 --verbose

# 4. Wait 30-45 minutes...

# 5. View results
cat mapping.md              # Read the analysis
open derived_views/character_network.png  # See character relationships
```

## Quick Test Before Full Run

To verify everything works before the long analysis:

```bash
# Clean up
rm -rf story.json batches/ results/ output/ mapping* derived_views/

# Test with tiny file
python run_analysis.py tiny_test.txt --verbose

# Should complete in ~5 seconds
```