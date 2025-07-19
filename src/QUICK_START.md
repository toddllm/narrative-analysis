# Quick Start Guide - Narrative Analysis

## 1. Test Everything Works First

```bash
cd ~/monprime-work/narrative-analysis/src

# Quick test with tiny file
python run_analysis.py tiny_test.txt --clean --verbose

# Should complete in ~5 seconds
```

## 2. Run Full Analysis on Zeldina Story

```bash
cd ~/monprime-work/narrative-analysis/src

# Clean previous data and run full analysis
python run_analysis.py ~/monprime-work/zeldina-story.txt --clean --batch-size 20 --verbose
```

This will:
- Process ~10,000 text units from zeldina-story.txt
- Create ~500 batches of 20 units each
- Take approximately 30-45 minutes on your RTX 3090
- Show real-time progress with the --verbose flag

## 3. View Results

After completion, examine:
- `mapping.md` - Full narrative analysis report
- `derived_views/character_network.png` - Character relationship visualization
- `derived_views/summary_report.md` - High-level summary

## Common Issues

**"Found cached data from different file"**
→ Always use `--clean` when switching files

**"Story file not found"**
→ Use full path: `~/monprime-work/zeldina-story.txt`

**Process seems slow**
→ Normal: Each batch takes 10-15 seconds with real LLM

## Alternative: Web Interface

```bash
python analysis_server.py
# Open http://localhost:5000
# Enter: /home/tdeshane/monprime-work/zeldina-story.txt
```