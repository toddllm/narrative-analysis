#!/usr/bin/env python3
"""
Post-Processor for Zero-Loss Mapping Workflow
Generates cross-linked views and derived data from the master mapping
"""

import json
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple
from datetime import datetime
import networkx as nx
import matplotlib.pyplot as plt


class PostProcessor:
    def __init__(self, mapping_file: str = "mapping.json"):
        """
        Initialize post-processor with mapping data
        
        Args:
            mapping_file: Path to the merged mapping JSON file
        """
        self.mapping_file = Path(mapping_file)
        self.mapping_data = self._load_mapping()
        self.character_graph = nx.Graph()
        self.location_graph = nx.DiGraph()
        
    def _load_mapping(self) -> Dict[str, Any]:
        """Load the mapping data"""
        with open(self.mapping_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def generate_character_atlas(self) -> Dict[str, Any]:
        """Generate character relationship atlas"""
        character_data = {}
        character_interactions = []
        
        # Process each mapped unit
        for unit in self.mapping_data['mapping']:
            characters = unit.get('Characters', '')
            if characters and characters != 'N/A':
                char_list = [c.strip() for c in characters.split(',')]
                
                # Track character appearances
                for char in char_list:
                    if char not in character_data:
                        character_data[char] = {
                            'first_appearance': unit['UID'],
                            'appearances': [],
                            'locations': set(),
                            'key_items': set(),
                            'narrative_roles': []
                        }
                    
                    character_data[char]['appearances'].append(unit['UID'])
                    
                    # Add locations where character appears
                    locations = unit.get('Locations', '')
                    if locations and locations != 'N/A':
                        for loc in locations.split(','):
                            character_data[char]['locations'].add(loc.strip())
                    
                    # Add associated items
                    items = unit.get('Key Items/Concepts', '')
                    if items and items != 'N/A':
                        for item in items.split(','):
                            character_data[char]['key_items'].add(item.strip())
                    
                    # Add narrative purpose
                    purpose = unit.get('Narrative Purpose', '')
                    if purpose:
                        character_data[char]['narrative_roles'].append({
                            'uid': unit['UID'],
                            'purpose': purpose
                        })
                
                # Track character interactions (co-appearances)
                if len(char_list) > 1:
                    for i in range(len(char_list)):
                        for j in range(i+1, len(char_list)):
                            interaction = {
                                'char1': char_list[i],
                                'char2': char_list[j],
                                'uid': unit['UID'],
                                'context': unit.get('Raw Sentence', '')[:100] + '...'
                            }
                            character_interactions.append(interaction)
                            
                            # Add to graph
                            self.character_graph.add_edge(char_list[i], char_list[j])
        
        # Convert sets to lists for JSON serialization
        for char in character_data:
            character_data[char]['locations'] = list(character_data[char]['locations'])
            character_data[char]['key_items'] = list(character_data[char]['key_items'])
            character_data[char]['appearance_count'] = len(character_data[char]['appearances'])
        
        return {
            'characters': character_data,
            'interactions': character_interactions,
            'total_characters': len(character_data)
        }
    
    def generate_location_gazetteer(self) -> Dict[str, Any]:
        """Generate location gazetteer with connections"""
        location_data = {}
        location_connections = []
        
        # Process each mapped unit
        for unit in self.mapping_data['mapping']:
            locations = unit.get('Locations', '')
            if locations and locations != 'N/A':
                loc_list = [l.strip() for l in locations.split(',')]
                
                for loc in loc_list:
                    if loc not in location_data:
                        location_data[loc] = {
                            'first_mention': unit['UID'],
                            'mentions': [],
                            'characters_present': set(),
                            'key_items': set(),
                            'narrative_events': []
                        }
                    
                    location_data[loc]['mentions'].append(unit['UID'])
                    
                    # Add characters at this location
                    characters = unit.get('Characters', '')
                    if characters and characters != 'N/A':
                        for char in characters.split(','):
                            location_data[loc]['characters_present'].add(char.strip())
                    
                    # Add items at this location
                    items = unit.get('Key Items/Concepts', '')
                    if items and items != 'N/A':
                        for item in items.split(','):
                            location_data[loc]['key_items'].add(item.strip())
                    
                    # Add narrative events
                    location_data[loc]['narrative_events'].append({
                        'uid': unit['UID'],
                        'purpose': unit.get('Narrative Purpose', ''),
                        'chapter': unit.get('chapter', 0)
                    })
        
        # Identify location connections based on narrative flow
        prev_locations = []
        for unit in self.mapping_data['mapping']:
            locations = unit.get('Locations', '')
            if locations and locations != 'N/A':
                curr_locations = [l.strip() for l in locations.split(',')]
                
                # Connect to previous locations
                for prev_loc in prev_locations:
                    for curr_loc in curr_locations:
                        if prev_loc != curr_loc:
                            connection = {
                                'from': prev_loc,
                                'to': curr_loc,
                                'uid': unit['UID'],
                                'chapter': unit.get('chapter', 0)
                            }
                            location_connections.append(connection)
                            self.location_graph.add_edge(prev_loc, curr_loc)
                
                prev_locations = curr_locations
        
        # Convert sets to lists
        for loc in location_data:
            location_data[loc]['characters_present'] = list(location_data[loc]['characters_present'])
            location_data[loc]['key_items'] = list(location_data[loc]['key_items'])
            location_data[loc]['mention_count'] = len(location_data[loc]['mentions'])
        
        return {
            'locations': location_data,
            'connections': location_connections,
            'total_locations': len(location_data)
        }
    
    def generate_item_inventory(self) -> Dict[str, Any]:
        """Generate inventory of key items and concepts"""
        item_data = {}
        
        for unit in self.mapping_data['mapping']:
            items = unit.get('Key Items/Concepts', '')
            if items and items != 'N/A':
                item_list = [i.strip() for i in items.split(',')]
                
                for item in item_list:
                    if item not in item_data:
                        item_data[item] = {
                            'first_mention': unit['UID'],
                            'mentions': [],
                            'associated_characters': set(),
                            'associated_locations': set(),
                            'narrative_contexts': []
                        }
                    
                    item_data[item]['mentions'].append(unit['UID'])
                    
                    # Add associated characters
                    characters = unit.get('Characters', '')
                    if characters and characters != 'N/A':
                        for char in characters.split(','):
                            item_data[item]['associated_characters'].add(char.strip())
                    
                    # Add associated locations
                    locations = unit.get('Locations', '')
                    if locations and locations != 'N/A':
                        for loc in locations.split(','):
                            item_data[item]['associated_locations'].add(loc.strip())
                    
                    # Add narrative context
                    item_data[item]['narrative_contexts'].append({
                        'uid': unit['UID'],
                        'purpose': unit.get('Narrative Purpose', ''),
                        'chapter': unit.get('chapter', 0)
                    })
        
        # Convert sets to lists and add statistics
        for item in item_data:
            item_data[item]['associated_characters'] = list(item_data[item]['associated_characters'])
            item_data[item]['associated_locations'] = list(item_data[item]['associated_locations'])
            item_data[item]['mention_count'] = len(item_data[item]['mentions'])
        
        return {
            'items': item_data,
            'total_items': len(item_data),
            'categories': self._categorize_items(item_data)
        }
    
    def _categorize_items(self, item_data: Dict[str, Any]) -> Dict[str, List[str]]:
        """Categorize items based on keywords"""
        categories = {
            'weapons': [],
            'defenses': [],
            'entities': [],
            'abilities': [],
            'locations': [],
            'concepts': []
        }
        
        for item in item_data:
            item_lower = item.lower()
            
            if any(word in item_lower for word in ['cannon', 'weapon', 'gun', 'blast']):
                categories['weapons'].append(item)
            elif any(word in item_lower for word in ['shield', 'armor', 'barrier', 'protection']):
                categories['defenses'].append(item)
            elif any(word in item_lower for word in ['zombie', 'boss', 'npc', 'entity']):
                categories['entities'].append(item)
            elif any(word in item_lower for word in ['ability', 'power', 'skill', 'magic']):
                categories['abilities'].append(item)
            elif any(word in item_lower for word in ['city', 'factory', 'dimension', 'star']):
                categories['locations'].append(item)
            else:
                categories['concepts'].append(item)
        
        return categories
    
    def generate_narrative_flow(self) -> Dict[str, Any]:
        """Generate narrative flow analysis"""
        chapters = {}
        narrative_arcs = []
        
        # Group by chapters
        for unit in self.mapping_data['mapping']:
            chapter = unit.get('chapter', 0)
            if chapter not in chapters:
                chapters[chapter] = {
                    'units': [],
                    'characters': set(),
                    'locations': set(),
                    'key_items': set(),
                    'word_count': 0
                }
            
            chapters[chapter]['units'].append(unit['UID'])
            chapters[chapter]['word_count'] += unit.get('word_count', 0)
            
            # Aggregate chapter elements
            characters = unit.get('Characters', '')
            if characters and characters != 'N/A':
                for char in characters.split(','):
                    chapters[chapter]['characters'].add(char.strip())
            
            locations = unit.get('Locations', '')
            if locations and locations != 'N/A':
                for loc in locations.split(','):
                    chapters[chapter]['locations'].add(loc.strip())
            
            items = unit.get('Key Items/Concepts', '')
            if items and items != 'N/A':
                for item in items.split(','):
                    chapters[chapter]['key_items'].add(item.strip())
        
        # Convert sets to lists
        for ch in chapters:
            chapters[ch]['characters'] = list(chapters[ch]['characters'])
            chapters[ch]['locations'] = list(chapters[ch]['locations'])
            chapters[ch]['key_items'] = list(chapters[ch]['key_items'])
            chapters[ch]['unit_count'] = len(chapters[ch]['units'])
        
        # Identify narrative arcs (simplified)
        arc_keywords = {
            'setup': ['assemble', 'reveal', 'discover'],
            'conflict': ['battle', 'fight', 'infection', 'transform'],
            'resolution': ['defeat', 'restore', 'memorial', 'end']
        }
        
        for unit in self.mapping_data['mapping']:
            purpose = unit.get('Narrative Purpose', '').lower()
            for arc_type, keywords in arc_keywords.items():
                if any(kw in purpose for kw in keywords):
                    narrative_arcs.append({
                        'uid': unit['UID'],
                        'chapter': unit.get('chapter', 0),
                        'arc_type': arc_type,
                        'purpose': unit.get('Narrative Purpose', '')
                    })
        
        return {
            'chapters': chapters,
            'narrative_arcs': narrative_arcs,
            'total_chapters': len(chapters),
            'total_word_count': sum(ch['word_count'] for ch in chapters.values())
        }
    
    def save_all_views(self, output_dir: str = "derived_views"):
        """Save all derived views"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Generate all views
        views = {
            'character_atlas': self.generate_character_atlas(),
            'location_gazetteer': self.generate_location_gazetteer(),
            'item_inventory': self.generate_item_inventory(),
            'narrative_flow': self.generate_narrative_flow()
        }
        
        # Save each view
        for view_name, view_data in views.items():
            output_file = output_path / f"{view_name}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(view_data, f, indent=2, ensure_ascii=False)
            print(f"✓ Saved {view_name} to {output_file}")
        
        # Generate visualizations
        self.generate_visualizations(output_path)
        
        # Generate summary report
        self.generate_summary_report(views, output_path)
    
    def generate_visualizations(self, output_dir: Path):
        """Generate network visualizations"""
        # Character interaction network
        if len(self.character_graph.nodes) > 0:
            plt.figure(figsize=(12, 8))
            pos = nx.spring_layout(self.character_graph, k=2, iterations=50)
            
            # Node sizes based on degree
            node_sizes = [300 * self.character_graph.degree(n) for n in self.character_graph.nodes]
            
            nx.draw(self.character_graph, pos, 
                   with_labels=True,
                   node_size=node_sizes,
                   node_color='lightblue',
                   font_size=10,
                   font_weight='bold',
                   edge_color='gray',
                   alpha=0.7)
            
            plt.title("Character Interaction Network", fontsize=16)
            plt.tight_layout()
            plt.savefig(output_dir / "character_network.png", dpi=150)
            plt.close()
            
            # Export as GraphML for interactive analysis (Gephi/Cytoscape)
            # nx.write_graphml(self.character_graph, output_dir / "character_network.graphml")  # NumPy 2.0 compatibility issue
            print("✓ Saved character network visualization")
        
        # Location flow diagram
        if len(self.location_graph.nodes) > 0:
            plt.figure(figsize=(14, 10))
            pos = nx.spring_layout(self.location_graph, k=3, iterations=50)
            
            nx.draw(self.location_graph, pos,
                   with_labels=True,
                   node_size=1000,
                   node_color='lightgreen',
                   font_size=9,
                   font_weight='bold',
                   edge_color='darkgreen',
                   arrows=True,
                   alpha=0.7)
            
            plt.title("Location Flow Network", fontsize=16)
            plt.tight_layout()
            plt.savefig(output_dir / "location_flow.png", dpi=150)
            plt.close()
            print("✓ Saved location flow visualization")
    
    def generate_summary_report(self, views: Dict[str, Any], output_dir: Path):
        """Generate a summary report of all derived views"""
        report = f"""# Zero-Loss Mapping - Derived Views Summary

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Character Atlas
- Total Characters: {views['character_atlas']['total_characters']}
- Total Interactions: {len(views['character_atlas']['interactions'])}

### Top Characters by Appearances:
"""
        # Sort characters by appearance count
        char_data = views['character_atlas']['characters']
        sorted_chars = sorted(char_data.items(), 
                            key=lambda x: x[1]['appearance_count'], 
                            reverse=True)[:10]
        
        for char, data in sorted_chars:
            report += f"- **{char}**: {data['appearance_count']} appearances\n"
        
        report += f"""
## Location Gazetteer
- Total Locations: {views['location_gazetteer']['total_locations']}
- Total Connections: {len(views['location_gazetteer']['connections'])}

### Top Locations by Mentions:
"""
        # Sort locations
        loc_data = views['location_gazetteer']['locations']
        sorted_locs = sorted(loc_data.items(),
                           key=lambda x: x[1]['mention_count'],
                           reverse=True)[:10]
        
        for loc, data in sorted_locs:
            report += f"- **{loc}**: {data['mention_count']} mentions\n"
        
        report += f"""
## Item Inventory
- Total Items/Concepts: {views['item_inventory']['total_items']}

### Categories:
"""
        for category, items in views['item_inventory']['categories'].items():
            if items:
                report += f"- **{category.capitalize()}**: {len(items)} items\n"
        
        report += f"""
## Narrative Flow
- Total Chapters: {views['narrative_flow']['total_chapters']}
- Total Word Count: {views['narrative_flow']['total_word_count']:,}

### Chapter Summary:
"""
        for ch_num, ch_data in sorted(views['narrative_flow']['chapters'].items()):
            if ch_num > 0:  # Skip chapter 0 (pre-chapter content)
                report += f"- **Chapter {ch_num}**: {ch_data['unit_count']} units, {ch_data['word_count']:,} words\n"
        
        # Save report
        report_file = output_dir / "summary_report.md"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"✓ Saved summary report to {report_file}")


def main():
    """Main entry point"""
    print("Post-Processor for Zero-Loss Mapping")
    print("="*50)
    
    # Check if mapping exists
    if not Path("mapping.json").exists():
        print("Error: mapping.json not found. Run orchestrator.py first.")
        return
    
    # Process
    processor = PostProcessor("mapping.json")
    processor.save_all_views("derived_views")
    
    print("\nPost-processing complete!")
    print("Check the 'derived_views' directory for:")
    print("  - character_atlas.json")
    print("  - location_gazetteer.json")
    print("  - item_inventory.json")
    print("  - narrative_flow.json")
    print("  - Network visualizations (PNG)")
    print("  - summary_report.md")


if __name__ == "__main__":
    main()