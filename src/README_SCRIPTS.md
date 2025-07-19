# Narrative Analysis Scripts

This directory contains two scripts for running narrative analysis:

## 1. CLI Script: `run_analysis.py`

A command-line script with colored output and progress tracking.

### Basic Usage:
```bash
# Run with default settings (Qwen2.5:32b model)
python run_analysis.py ../../zeldina-story.txt

# Use mock LLM for quick testing
python run_analysis.py ../../zeldina-story.txt --mock

# Enable verbose output
python run_analysis.py ../../zeldina-story.txt --verbose

# Use different model with custom batch size
python run_analysis.py ../../zeldina-story.txt --model llama3.1:8b --batch-size 5
```

### Features:
- Colored terminal output
- Progress bars for batch processing
- Elapsed time tracking
- Graceful cancellation with Ctrl+C
- Summary of generated files

## 2. Web Server: `analysis_server.py`

A web-based interface for monitoring and controlling analysis.

### Usage:
```bash
# Start server on default port (5000)
python analysis_server.py

# Start on custom port with verbose logging
python analysis_server.py --port 8080 --verbose

# Make server accessible from other machines
python analysis_server.py --host 0.0.0.0
```

### Features:
- Real-time progress monitoring
- Live log streaming
- Start/stop analysis from web interface
- Toggle verbose logging on/off
- Visual progress bar
- Color-coded log levels

### Web Interface:
Open http://localhost:5000 in your browser to:
- Configure analysis parameters
- Start/cancel analysis
- Monitor progress in real-time
- View live logs with filtering

## Generated Output Files

Both scripts generate the same output files:

1. **mapping.json** - Structured narrative data
2. **mapping.csv** - Spreadsheet format for data analysis
3. **mapping.md** - Human-readable markdown report
4. **derived_views/character_network.png** - Character relationship visualization
5. **derived_views/location_flow.png** - Location connection map
6. **derived_views/narrative_flow.json** - Story progression data
7. **gap_report.json** - Missing UID report (if any gaps found)

## Performance Notes

- Mock LLM: ~1-2 minutes for a full novel
- Qwen2.5:32b: ~15-30 seconds per batch (depends on GPU)
- Qwen2.5:72b: ~30-60 seconds per batch (requires 64GB+ RAM)
- Llama3.1:8b: ~5-10 seconds per batch (faster but less accurate)

## Tips

1. Use `--mock` first to test the pipeline quickly
2. Start with smaller `--batch-size` (5-10) for better accuracy
3. Use the web server for long-running analyses
4. Enable `--verbose` to debug issues
5. Check GPU memory usage with `nvidia-smi` during processing