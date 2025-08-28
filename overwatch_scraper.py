#!/usr/bin/env python3

import requests
import json
import pandas as pd
import csv
import logging
import argparse
import time
import os
from itertools import product
from typing import Dict, List, Any, Optional
import sys

class OverwatchScraper:
    """
    Scraper for Overwatch 2 hero statistics from official Blizzard API.
    """
    
    BASE_URL = "https://overwatch.blizzard.com/en-us/rates/data/"
    
    # Parameter definitions based on the spec
    INPUTS = ["PC", "Controller"]
    MAPS = ["all-maps"]  # Can be extended with specific map names
    REGIONS = ["Europe", "US", "Asia"]
    ROLES = ["All", "Damage", "Tank", "Support"]
    RQ_OPTIONS = [0, 1]  # Role queue flag
    TIERS = ["All", "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Master", "Grandmaster"]
    
    def __init__(self, output_dir: str = "data", delay: float = 1.0):
        """
        Initialize the scraper.
        
        Args:
            output_dir: Directory to save CSV files
            delay: Delay between requests in seconds
        """
        self.output_dir = output_dir
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('overwatch_scraper.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def build_url(self, input_type: str, map_name: str, region: str, 
                  role: str, rq: int, tier: str) -> str:
        """
        Build API URL with query parameters.
        
        Args:
            input_type: Input method (PC, Controller)
            map_name: Map name or "all-maps"
            region: Region name
            role: Role filter
            rq: Role queue flag
            tier: Competitive tier
            
        Returns:
            Complete API URL
        """
        params = {
            'input': input_type,
            'map': map_name,
            'region': region,
            'role': role,
            'rq': rq,
            'tier': tier
        }
        
        query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
        return f"{self.BASE_URL}?{query_string}"
    
    def fetch_data(self, url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Fetch JSON data from URL with retry logic.
        
        Args:
            url: API URL to fetch
            max_retries: Maximum number of retry attempts
            
        Returns:
            Parsed JSON data or None if failed
        """
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Fetching: {url}")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                # Add delay to be respectful to the API
                time.sleep(self.delay)
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.logger.error(f"Failed to fetch data after {max_retries} attempts: {url}")
                    return None
    
    def flatten_json(self, data: Dict[str, Any], prefix: str = '') -> Dict[str, Any]:
        """
        Flatten nested JSON structure for CSV export.
        
        Args:
            data: JSON data to flatten
            prefix: Prefix for nested keys
            
        Returns:
            Flattened dictionary
        """
        flattened = {}
        
        for key, value in data.items():
            new_key = f"{prefix}_{key}" if prefix else key
            
            if isinstance(value, dict):
                flattened.update(self.flatten_json(value, new_key))
            elif isinstance(value, list):
                # Handle lists by converting to string or creating indexed entries
                if all(isinstance(item, (str, int, float, bool)) for item in value):
                    flattened[new_key] = json.dumps(value)
                else:
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            flattened.update(self.flatten_json(item, f"{new_key}_{i}"))
                        else:
                            flattened[f"{new_key}_{i}"] = item
            else:
                flattened[new_key] = value
                
        return flattened
    
    def process_data(self, data: Dict[str, Any], params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Normalize API JSON into one record per hero with request metadata.
        
        Args:
            data: Raw JSON payload from the API (or example file)
            params: Request parameters used
            
        Returns:
            List of processed records (one per hero when possible)
        """
        records: List[Dict[str, Any]] = []
        
        metadata = {
            'input_type': params['input'],
            'map_name': params['map'],
            'region': params['region'],
            'role': params['role'],
            'rq': params['rq'],
            'tier': params['tier'],
            'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Prefer a list of hero entries under the expected key
        rates_list: Optional[List[Dict[str, Any]]] = None
        if isinstance(data, dict):
            if isinstance(data.get('rates'), list) and data.get('rates') and isinstance(data['rates'][0], dict):
                rates_list = data['rates']
            else:
                # Fallback: search for a list of hero dicts in any top-level field
                for value in data.values():
                    if isinstance(value, list) and value and isinstance(value[0], dict):
                        sample = value[0]
                        if any(k in sample for k in ('id', 'cells', 'hero')):
                            rates_list = value
                            break
        
        if rates_list is not None:
            for index, hero_data in enumerate(rates_list):
                if not isinstance(hero_data, dict):
                    continue
                record = metadata.copy()
                # Flatten hero record (e.g., id, cells_*, hero_*)
                record.update(self.flatten_json(hero_data))
                # Keep the index within the rates list for traceability
                record['hero_index'] = index
                records.append(record)
        else:
            # Last-resort fallback: flatten the whole payload as a single record
            self.logger.warning("No hero list found in payload; flattening entire JSON as a single record")
            record = metadata.copy()
            record.update(self.flatten_json(data))
            records.append(record)
        
        return records
    
    def generate_filename(self, params: Dict[str, Any]) -> str:
        """
        Generate CSV filename based on parameters.
        
        Args:
            params: Request parameters
            
        Returns:
            CSV filename
        """
        filename = (
            f"stats_input-{params['input']}_"
            f"map-{params['map']}_"
            f"region-{params['region']}_"
            f"role-{params['role']}_"
            f"rq-{params['rq']}_"
            f"tier-{params['tier']}.csv"
        )
        return filename.replace(' ', '-').lower()
    
    def save_to_csv(self, records: List[Dict[str, Any]], filename: str) -> None:
        """
        Save records to CSV file.
        
        Args:
            records: List of record dictionaries
            filename: Output filename
        """
        if not records:
            self.logger.warning(f"No records to save for {filename}")
            return
        
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            df = pd.DataFrame(records)
            df.to_csv(filepath, index=False)
            self.logger.info(f"Saved {len(records)} records to {filepath}")
        except Exception as e:
            self.logger.error(f"Failed to save {filepath}: {e}")
    
    def scrape_all_combinations(self, limit_combinations: Optional[int] = None) -> None:
        """
        Scrape data for all parameter combinations.
        
        Args:
            limit_combinations: Limit number of combinations to process (for testing)
        """
        combinations = list(product(
            self.INPUTS, self.MAPS, self.REGIONS, 
            self.ROLES, self.RQ_OPTIONS, self.TIERS
        ))
        
        total_combinations = len(combinations)
        if limit_combinations:
            combinations = combinations[:limit_combinations]
            self.logger.info(f"Limited to first {limit_combinations} combinations")
        
        self.logger.info(f"Starting scrape of {len(combinations)} combinations out of {total_combinations} total")
        
        success_count = 0
        error_count = 0
        
        for i, (input_type, map_name, region, role, rq, tier) in enumerate(combinations, 1):
            params = {
                'input': input_type,
                'map': map_name,
                'region': region,
                'role': role,
                'rq': rq,
                'tier': tier
            }
            
            self.logger.info(f"Processing combination {i}/{len(combinations)}: {params}")
            
            url = self.build_url(input_type, map_name, region, role, rq, tier)
            data = self.fetch_data(url)
            
            if data is not None:
                records = self.process_data(data, params)
                filename = self.generate_filename(params)
                self.save_to_csv(records, filename)
                success_count += 1
            else:
                error_count += 1
        
        self.logger.info(f"Scraping completed. Success: {success_count}, Errors: {error_count}")
    
    def scrape_single(self, input_type: str, map_name: str, region: str, 
                     role: str, rq: int, tier: str) -> None:
        """
        Scrape data for a single parameter combination.
        """
        params = {
            'input': input_type,
            'map': map_name,
            'region': region,
            'role': role,
            'rq': rq,
            'tier': tier
        }
        
        url = self.build_url(input_type, map_name, region, role, rq, tier)
        data = self.fetch_data(url)
        
        if data is not None:
            records = self.process_data(data, params)
            filename = self.generate_filename(params)
            self.save_to_csv(records, filename)
            self.logger.info("Single scrape completed successfully")
        else:
            self.logger.error("Single scrape failed")


def main():
    parser = argparse.ArgumentParser(description="Overwatch 2 Hero Statistics Scraper")
    parser.add_argument("--output-dir", default="data", help="Output directory for CSV files")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    parser.add_argument("--limit", type=int, help="Limit number of combinations to process")
    parser.add_argument("--single", action="store_true", help="Scrape single combination")
    parser.add_argument("--input", choices=OverwatchScraper.INPUTS, help="Input type for single scrape")
    parser.add_argument("--map", default="all-maps", help="Map name for single scrape")
    parser.add_argument("--region", choices=OverwatchScraper.REGIONS, help="Region for single scrape")
    parser.add_argument("--role", choices=OverwatchScraper.ROLES, help="Role for single scrape")
    parser.add_argument("--rq", type=int, choices=[0, 1], help="Role queue flag for single scrape")
    parser.add_argument("--tier", choices=OverwatchScraper.TIERS, help="Tier for single scrape")
    
    args = parser.parse_args()
    
    scraper = OverwatchScraper(output_dir=args.output_dir, delay=args.delay)
    
    if args.single:
        # Validate required parameters for single scrape
        required_params = ['input', 'region', 'role', 'rq', 'tier']
        missing_params = [param for param in required_params if getattr(args, param) is None]
        
        if missing_params:
            print(f"Error: Missing required parameters for single scrape: {', '.join(missing_params)}")
            sys.exit(1)
        
        scraper.scrape_single(
            args.input, args.map, args.region, 
            args.role, args.rq, args.tier
        )
    else:
        scraper.scrape_all_combinations(limit_combinations=args.limit)


if __name__ == "__main__":
    main()