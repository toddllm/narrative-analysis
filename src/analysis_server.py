#!/usr/bin/env python3
"""
Narrative Analysis Server
A web-based interface for running and monitoring narrative analysis with real-time progress
"""

import json
import os
import sys
import time
import threading
import queue
import argparse
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, jsonify, request, Response
from flask_cors import CORS
import subprocess
import signal

# Import our modules
from orchestrator import MappingOrchestrator
from ingest import StoryIngestor
from chunk_dispatcher import ChunkDispatcher
from verifier import MappingVerifier
from merge_chunks import ChunkMerger
from post_processor import PostProcessor
from gap_detector import GapDetector

app = Flask(__name__)
CORS(app)

class AnalysisServer:
    def __init__(self):
        self.current_task = None
        self.progress = 0
        self.total_steps = 0
        self.status = "idle"
        self.verbose = False
        self.log_queue = queue.Queue()
        self.start_time = None
        self.end_time = None
        self.error = None
        self.analysis_thread = None
        self.cancel_requested = False
        
        # Analysis parameters
        self.story_file = None
        self.model_name = "qwen2.5:32b"
        self.batch_size = 10
        self.use_mock_llm = False
        
    def log(self, message, level="info"):
        """Add message to log queue"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "timestamp": timestamp,
            "level": level,
            "message": message
        }
        self.log_queue.put(log_entry)
        
        # Also print to console if verbose
        if self.verbose or level in ["error", "warning"]:
            print(f"[{timestamp}] [{level.upper()}] {message}")
    
    def get_logs(self, limit=100):
        """Get recent logs"""
        logs = []
        while not self.log_queue.empty() and len(logs) < limit:
            try:
                logs.append(self.log_queue.get_nowait())
            except queue.Empty:
                break
        return logs
    
    def run_analysis(self):
        """Run the narrative analysis pipeline"""
        try:
            self.status = "running"
            self.start_time = datetime.now()
            self.cancel_requested = False
            self.log(f"Starting analysis of {self.story_file}", "info")
            
            # Step 1: Ingest story
            self.current_task = "Ingesting story"
            self.log("Step 1/6: Ingesting story...", "info")
            
            if Path("story.json").exists():
                self.log("Using existing story.json", "info")
            else:
                ingestor = StoryIngestor(self.story_file)
                ingestor.process_story()
                ingestor.save_to_json("story.json")
                self.log(f"Ingested {len(ingestor.story_data['data'])} text units", "info")
            
            if self.cancel_requested:
                self.log("Analysis cancelled by user", "warning")
                return
            
            # Step 2: Create batches
            self.current_task = "Creating batches"
            self.log("Step 2/6: Creating batches...", "info")
            
            dispatcher = ChunkDispatcher("story.json", self.batch_size)
            batches = dispatcher.create_batches()
            dispatcher.save_all_batches("batches")
            
            self.total_steps = len(batches)
            self.log(f"Created {len(batches)} batches of {self.batch_size} units each", "info")
            
            if self.cancel_requested:
                self.log("Analysis cancelled by user", "warning")
                return
            
            # Step 3: Process batches
            self.current_task = "Processing batches"
            self.log("Step 3/6: Processing batches with LLM...", "info")
            
            orchestrator = MappingOrchestrator(
                story_file=self.story_file,
                batch_size=self.batch_size,
                use_mock_llm=self.use_mock_llm,
                model_name=self.model_name
            )
            
            # Process each batch
            for i, batch in enumerate(batches):
                if self.cancel_requested:
                    self.log("Analysis cancelled by user", "warning")
                    return
                    
                self.progress = i + 1
                batch_id = batch['batch_id']
                
                if self.verbose:
                    self.log(f"Processing {batch_id} ({i+1}/{len(batches)})", "debug")
                
                result = orchestrator.process_batch(batch)
                
                if result:
                    self.log(f"✓ {batch_id} processed successfully", "debug")
                else:
                    self.log(f"✗ {batch_id} failed processing", "warning")
            
            if self.cancel_requested:
                self.log("Analysis cancelled by user", "warning")
                return
            
            # Step 4: Merge results
            self.current_task = "Merging results"
            self.log("Step 4/6: Merging results...", "info")
            
            merger = ChunkMerger("results")
            merger.merge_all_results()
            merger.enrich_with_metadata()
            merger.save_mappings("mapping")
            stats = merger.merge_stats
            
            self.log(f"Merged {stats['total_units']} units from {stats['batches_processed']} batches", "info")
            
            if self.cancel_requested:
                self.log("Analysis cancelled by user", "warning")
                return
            
            # Step 5: Post-processing
            self.current_task = "Generating visualizations"
            self.log("Step 5/6: Generating visualizations and reports...", "info")
            
            processor = PostProcessor("mapping.json")
            processor.save_all_views("derived_views")
            
            self.log("Generated character network, location flow, and derived views", "info")
            
            if self.cancel_requested:
                self.log("Analysis cancelled by user", "warning")
                return
            
            # Step 6: Gap detection
            self.current_task = "Verifying integrity"
            self.log("Step 6/6: Running gap detection...", "info")
            
            detector = GapDetector("story.json", "mapping.json")
            missing_uids = detector.detect_missing_uids()
            
            if missing_uids:
                self.log(f"Warning: {len(missing_uids)} UIDs missing from analysis", "warning")
            else:
                self.log("✓ 100% coverage achieved - no gaps detected", "success")
            
            # Complete
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            self.log(f"Analysis completed in {duration:.1f} seconds", "success")
            self.status = "completed"
            self.current_task = "Analysis complete"
            
        except Exception as e:
            self.error = str(e)
            self.status = "error"
            self.log(f"Error during analysis: {str(e)}", "error")
            raise
    
    def start_analysis(self, story_file, model_name=None, batch_size=None, use_mock=False):
        """Start analysis in background thread"""
        if self.status == "running":
            return False, "Analysis already running"
        
        self.story_file = story_file
        if model_name:
            self.model_name = model_name
        if batch_size:
            self.batch_size = batch_size
        self.use_mock_llm = use_mock
        
        # Reset state
        self.progress = 0
        self.total_steps = 0
        self.error = None
        self.current_task = "Initializing"
        
        # Start analysis thread
        self.analysis_thread = threading.Thread(target=self.run_analysis)
        self.analysis_thread.daemon = True
        self.analysis_thread.start()
        
        return True, "Analysis started"
    
    def cancel_analysis(self):
        """Request cancellation of running analysis"""
        if self.status == "running":
            self.cancel_requested = True
            self.status = "cancelling"
            return True, "Cancellation requested"
        return False, "No analysis running"

# Create global server instance
server = AnalysisServer()

@app.route('/')
def index():
    """Serve the web interface"""
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Narrative Analysis Server</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f5f5f5;
            }
            .container {
                background-color: white;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            h1 {
                color: #333;
                border-bottom: 2px solid #007bff;
                padding-bottom: 10px;
            }
            .status-panel {
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 5px;
                padding: 20px;
                margin: 20px 0;
            }
            .progress-bar {
                width: 100%;
                height: 30px;
                background-color: #e9ecef;
                border-radius: 15px;
                overflow: hidden;
                margin: 10px 0;
            }
            .progress-fill {
                height: 100%;
                background-color: #007bff;
                transition: width 0.3s ease;
                display: flex;
                align-items: center;
                justify-content: center;
                color: white;
                font-weight: bold;
            }
            .controls {
                margin: 20px 0;
            }
            .controls input, .controls select {
                margin: 5px;
                padding: 8px;
                border: 1px solid #ced4da;
                border-radius: 4px;
            }
            .controls button {
                margin: 5px;
                padding: 10px 20px;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-weight: bold;
            }
            .btn-primary {
                background-color: #007bff;
                color: white;
            }
            .btn-danger {
                background-color: #dc3545;
                color: white;
            }
            .btn-secondary {
                background-color: #6c757d;
                color: white;
            }
            button:hover {
                opacity: 0.9;
            }
            button:disabled {
                opacity: 0.5;
                cursor: not-allowed;
            }
            .log-panel {
                background-color: #000;
                color: #0f0;
                padding: 15px;
                border-radius: 5px;
                height: 300px;
                overflow-y: auto;
                font-family: 'Courier New', monospace;
                font-size: 12px;
                margin-top: 20px;
            }
            .log-entry {
                margin: 2px 0;
            }
            .log-error { color: #ff0000; }
            .log-warning { color: #ffff00; }
            .log-success { color: #00ff00; }
            .log-debug { color: #808080; }
            .status-idle { color: #6c757d; }
            .status-running { color: #007bff; }
            .status-completed { color: #28a745; }
            .status-error { color: #dc3545; }
            .checkbox-label {
                margin-left: 5px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔬 Narrative Analysis Server</h1>
            
            <div class="status-panel">
                <h2>Status</h2>
                <p><strong>Current Status:</strong> <span id="status" class="status-idle">Idle</span></p>
                <p><strong>Current Task:</strong> <span id="task">None</span></p>
                <p><strong>Progress:</strong> <span id="progress-text">0 / 0</span></p>
                <div class="progress-bar">
                    <div class="progress-fill" id="progress-bar" style="width: 0%">0%</div>
                </div>
            </div>
            
            <div class="controls">
                <h2>Controls</h2>
                <div>
                    <label>Story File:</label>
                    <input type="text" id="story-file" placeholder="Path to story file" value="../../zeldina-story.txt" style="width: 300px;">
                </div>
                <div>
                    <label>Model:</label>
                    <select id="model-name">
                        <option value="qwen2.5:32b">Qwen2.5:32b (Recommended)</option>
                        <option value="qwen2.5:72b">Qwen2.5:72b (Requires 64GB+)</option>
                        <option value="llama3.1:8b">Llama3.1:8b (Faster)</option>
                        <option value="mock">Mock LLM (Testing)</option>
                    </select>
                    
                    <label>Batch Size:</label>
                    <input type="number" id="batch-size" value="10" min="1" max="50" style="width: 60px;">
                    
                    <label>
                        <input type="checkbox" id="verbose" onchange="toggleVerbose()">
                        <span class="checkbox-label">Verbose Logging</span>
                    </label>
                </div>
                <div>
                    <button class="btn-primary" onclick="startAnalysis()" id="start-btn">Start Analysis</button>
                    <button class="btn-danger" onclick="cancelAnalysis()" id="cancel-btn" disabled>Cancel</button>
                    <button class="btn-secondary" onclick="clearLogs()">Clear Logs</button>
                </div>
            </div>
            
            <div>
                <h2>Live Logs</h2>
                <div class="log-panel" id="log-panel"></div>
            </div>
        </div>
        
        <script>
            let updateInterval = null;
            
            function startAnalysis() {
                const storyFile = document.getElementById('story-file').value;
                const modelName = document.getElementById('model-name').value;
                const batchSize = document.getElementById('batch-size').value;
                const useMock = modelName === 'mock';
                
                fetch('/api/start', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        story_file: storyFile,
                        model_name: useMock ? 'qwen2.5:32b' : modelName,
                        batch_size: parseInt(batchSize),
                        use_mock: useMock
                    })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        document.getElementById('start-btn').disabled = true;
                        document.getElementById('cancel-btn').disabled = false;
                        startUpdating();
                    } else {
                        alert('Failed to start: ' + data.message);
                    }
                });
            }
            
            function cancelAnalysis() {
                fetch('/api/cancel', {method: 'POST'})
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        addLog('info', 'Cancellation requested...');
                    }
                });
            }
            
            function toggleVerbose() {
                const verbose = document.getElementById('verbose').checked;
                fetch('/api/verbose', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({verbose: verbose})
                });
            }
            
            function clearLogs() {
                document.getElementById('log-panel').innerHTML = '';
            }
            
            function addLog(level, message) {
                const logPanel = document.getElementById('log-panel');
                const entry = document.createElement('div');
                entry.className = 'log-entry log-' + level;
                const timestamp = new Date().toLocaleTimeString();
                entry.textContent = `[${timestamp}] [${level.toUpperCase()}] ${message}`;
                logPanel.appendChild(entry);
                logPanel.scrollTop = logPanel.scrollHeight;
            }
            
            function updateStatus() {
                fetch('/api/status')
                .then(response => response.json())
                .then(data => {
                    // Update status
                    const statusEl = document.getElementById('status');
                    statusEl.textContent = data.status.charAt(0).toUpperCase() + data.status.slice(1);
                    statusEl.className = 'status-' + data.status;
                    
                    // Update task
                    document.getElementById('task').textContent = data.current_task || 'None';
                    
                    // Update progress
                    const progressText = `${data.progress} / ${data.total_steps}`;
                    document.getElementById('progress-text').textContent = progressText;
                    
                    const percentage = data.total_steps > 0 ? 
                        Math.round((data.progress / data.total_steps) * 100) : 0;
                    const progressBar = document.getElementById('progress-bar');
                    progressBar.style.width = percentage + '%';
                    progressBar.textContent = percentage + '%';
                    
                    // Update buttons
                    if (data.status === 'running' || data.status === 'cancelling') {
                        document.getElementById('start-btn').disabled = true;
                        document.getElementById('cancel-btn').disabled = false;
                    } else {
                        document.getElementById('start-btn').disabled = false;
                        document.getElementById('cancel-btn').disabled = true;
                    }
                    
                    // Get new logs
                    data.logs.forEach(log => {
                        addLog(log.level, log.message);
                    });
                    
                    // Stop updating if completed or error
                    if (data.status === 'completed' || data.status === 'error') {
                        stopUpdating();
                    }
                });
            }
            
            function startUpdating() {
                updateStatus();
                updateInterval = setInterval(updateStatus, 1000);
            }
            
            function stopUpdating() {
                if (updateInterval) {
                    clearInterval(updateInterval);
                    updateInterval = null;
                }
            }
            
            // Initial update
            updateStatus();
        </script>
    </body>
    </html>
    '''

@app.route('/api/status')
def api_status():
    """Get current analysis status"""
    return jsonify({
        'status': server.status,
        'current_task': server.current_task,
        'progress': server.progress,
        'total_steps': server.total_steps,
        'verbose': server.verbose,
        'logs': server.get_logs()
    })

@app.route('/api/start', methods=['POST'])
def api_start():
    """Start new analysis"""
    data = request.json
    success, message = server.start_analysis(
        story_file=data.get('story_file'),
        model_name=data.get('model_name'),
        batch_size=data.get('batch_size'),
        use_mock=data.get('use_mock', False)
    )
    return jsonify({'success': success, 'message': message})

@app.route('/api/cancel', methods=['POST'])
def api_cancel():
    """Cancel running analysis"""
    success, message = server.cancel_analysis()
    return jsonify({'success': success, 'message': message})

@app.route('/api/verbose', methods=['POST'])
def api_verbose():
    """Toggle verbose logging"""
    data = request.json
    server.verbose = data.get('verbose', False)
    server.log(f"Verbose logging {'enabled' if server.verbose else 'disabled'}", "info")
    return jsonify({'success': True, 'verbose': server.verbose})

def signal_handler(sig, frame):
    """Handle shutdown gracefully"""
    print("\nShutting down server...")
    if server.status == "running":
        server.cancel_analysis()
    sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description='Narrative Analysis Server')
    parser.add_argument('--port', type=int, default=5000, help='Port to run server on')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging by default')
    
    args = parser.parse_args()
    
    if args.verbose:
        server.verbose = True
    
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    print(f"""
    ╔══════════════════════════════════════════╗
    ║     Narrative Analysis Server v1.0       ║
    ╠══════════════════════════════════════════╣
    ║  Server running at:                      ║
    ║  http://{args.host}:{args.port:<5}                        ║
    ║                                          ║
    ║  Press Ctrl+C to stop                    ║
    ╚══════════════════════════════════════════╝
    """)
    
    # Run Flask app
    app.run(host=args.host, port=args.port, debug=False)

if __name__ == '__main__':
    main()