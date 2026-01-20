"""
FPL API Client for fetching data from the official Fantasy Premier League API.

This module provides functions to fetch:
- Bootstrap static data (teams, players, events, element_types)
- Player-specific data by gameweek
- Fixtures data
- Historical player data
"""

import requests
import json
import pandas as pd
from typing import Dict, List, Optional
from pathlib import Path
import time


BASE_URL = "https://fantasy.premierleague.com/api"


class FPLAPIClient:
    """Client for interacting with the FPL API."""
    
    def __init__(self, base_url: str = BASE_URL, rate_limit_delay: float = 0.1):
        """
        Initialize FPL API client.
        
        Args:
            base_url: Base URL for FPL API
            rate_limit_delay: Delay between requests in seconds to respect rate limits
        """
        self.base_url = base_url
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'FPL-Data-Pipeline/1.0'
        })
    
    def _make_request(self, endpoint: str) -> Dict:
        """
        Make a GET request to the FPL API.
        
        Args:
            endpoint: API endpoint (relative to base_url)
            
        Returns:
            JSON response as dictionary
            
        Raises:
            requests.RequestException: If request fails
        """
        url = f"{self.base_url}/{endpoint}"
        time.sleep(self.rate_limit_delay)  # Rate limiting
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"Failed to fetch {url}: {str(e)}")
    
    def get_bootstrap_static(self) -> Dict:
        """
        Fetch bootstrap-static data containing teams, players, events, etc.
        
        Returns:
            Dictionary containing:
            - events: Gameweek information
            - teams: Team information
            - elements: Player information
            - element_types: Position information (GK, DEF, MID, FWD)
            - total_players: Total number of players
            - chips: Available chips
        """
        return self._make_request("bootstrap-static/")
    
    def get_player_data(self, player_id: int) -> Dict:
        """
        Fetch detailed data for a specific player.
        
        Args:
            player_id: FPL player ID
            
        Returns:
            Dictionary containing player history, fixtures, and summary
        """
        return self._make_request(f"element-summary/{player_id}/")
    
    def get_fixtures(self, event_id: Optional[int] = None) -> List[Dict]:
        """
        Fetch fixtures data.
        
        Args:
            event_id: Optional gameweek ID. If None, returns all fixtures.
            
        Returns:
            List of fixture dictionaries
        """
        if event_id:
            return self._make_request(f"fixtures/?event={event_id}")
        return self._make_request("fixtures/")
    
    def get_live_data(self, event_id: Optional[int] = None) -> Dict:
        """
        Fetch live data for a gameweek (points, bonus, etc.).
        
        Args:
            event_id: Optional gameweek ID. If None, returns current gameweek.
            
        Returns:
            Dictionary containing live gameweek data
        """
        if event_id:
            return self._make_request(f"event/{event_id}/live/")
        return self._make_request("event/live/")
    
    def get_gameweek_data(self, event_id: int) -> Dict:
        """
        Fetch gameweek-specific data.
        
        Args:
            event_id: Gameweek ID
            
        Returns:
            Dictionary containing gameweek information
        """
        return self._make_request(f"event/{event_id}/")
    
    def get_all_players_data(self, max_players: Optional[int] = None) -> List[Dict]:
        """
        Fetch detailed data for all players.
        
        Warning: This makes one request per player. Use with caution.
        
        Args:
            max_players: Optional limit on number of players to fetch (for testing)
            
        Returns:
            List of player data dictionaries
        """
        bootstrap = self.get_bootstrap_static()
        players = bootstrap['elements']
        
        if max_players:
            players = players[:max_players]
        
        all_player_data = []
        for player in players:
            player_id = player['id']
            try:
                player_data = self.get_player_data(player_id)
                all_player_data.append({
                    'player_id': player_id,
                    'data': player_data
                })
            except Exception as e:
                print(f"Failed to fetch data for player {player_id}: {e}")
                continue
        
        return all_player_data


def save_json(data: Dict, filepath: Path) -> None:
    """
    Save data to JSON file.
    
    Args:
        data: Data to save
        filepath: Path to save file
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)


def load_json(filepath: Path) -> Dict:
    """
    Load data from JSON file.
    
    Args:
        filepath: Path to JSON file
        
    Returns:
        Loaded data dictionary
    """
    with open(filepath, 'r') as f:
        return json.load(f)
