"""
Data transformation modules to convert raw FPL API data into structured tables.

This module creates the following tables as specified in the README:
- player_match_stats_raw: Raw per-match player statistics
- team_match_stats: Team-level match metrics
- fixtures: Match scheduling and difficulty context
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime


def create_player_match_stats_raw(
    bootstrap_data: Dict,
    all_players_data: List[Dict],
    season: str
) -> pd.DataFrame:
    """
    Create player_match_stats_raw table from FPL API data.
    
    This table contains raw per-match player statistics including:
    - minutes, total_points
    - goals, assists, bonus, BPS
    - xG, xA (if available from other sources)
    
    Args:
        bootstrap_data: Bootstrap static data from FPL API
        all_players_data: List of player data dictionaries from get_all_players_data
        season: Season identifier (e.g., '2024-25')
        
    Returns:
        DataFrame with columns: player_id, season, gameweek, match_id, 
        minutes, total_points, goals, assists, bonus, bps, etc.
    """
    records = []
    
    # Create player lookup
    players_lookup = {p['id']: p for p in bootstrap_data['elements']}
    
    for player_entry in all_players_data:
        player_id = player_entry['player_id']
        player_data = player_entry['data']
        
        # Extract player history (past matches)
        history = player_data.get('history', [])
        
        for match in history:
            record = {
                'player_id': player_id,
                'season': season,
                'gameweek': match.get('round'),
                'match_id': match.get('id'),  # FPL match ID
                'opponent_team_id': match.get('opponent_team'),
                'was_home': match.get('was_home', False),
                'minutes': match.get('minutes', 0),
                'total_points': match.get('total_points', 0),
                'goals_scored': match.get('goals_scored', 0),
                'assists': match.get('assists', 0),
                'clean_sheets': match.get('clean_sheets', 0),
                'goals_conceded': match.get('goals_conceded', 0),
                'own_goals': match.get('own_goals', 0),
                'penalties_saved': match.get('penalties_saved', 0),
                'penalties_missed': match.get('penalties_missed', 0),
                'yellow_cards': match.get('yellow_cards', 0),
                'red_cards': match.get('red_cards', 0),
                'saves': match.get('saves', 0),
                'bonus': match.get('bonus', 0),
                'bps': match.get('bps', 0),
                'influence': match.get('influence', 0.0),
                'creativity': match.get('creativity', 0.0),
                'threat': match.get('threat', 0.0),
                'ict_index': match.get('ict_index', 0.0),
                'value': match.get('value', 0),
                'transfers_balance': match.get('transfers_balance', 0),
                'selected': match.get('selected', 0),
                'transfers_in': match.get('transfers_in', 0),
                'transfers_out': match.get('transfers_out', 0),
            }
            
            # Add player metadata
            if player_id in players_lookup:
                player_info = players_lookup[player_id]
                record['element_type'] = player_info.get('element_type')  # Position
                record['team_id'] = player_info.get('team')
            
            records.append(record)
    
    df = pd.DataFrame(records)
    
    # Sort by player_id, season, gameweek
    if not df.empty:
        df = df.sort_values(['player_id', 'season', 'gameweek']).reset_index(drop=True)
    
    return df


def create_fixtures_table(
    fixtures_data: List[Dict],
    bootstrap_data: Dict,
    season: str
) -> pd.DataFrame:
    """
    Create fixtures table from FPL API data.
    
    This table contains match scheduling and difficulty context:
    - team vs opponent
    - home/away
    - fixture difficulty rating
    - days of rest
    - blank and double gameweeks
    
    Args:
        fixtures_data: List of fixture dictionaries from FPL API
        bootstrap_data: Bootstrap static data (for team names)
        season: Season identifier
        
    Returns:
        DataFrame with fixture information
    """
    if not fixtures_data:
        return pd.DataFrame()
    
    # Create team lookup
    teams_lookup = {t['id']: t for t in bootstrap_data['teams']}
    
    records = []
    for fixture in fixtures_data:
        record = {
            'match_id': fixture.get('id'),
            'season': season,
            'gameweek': fixture.get('event'),
            'team_id': fixture.get('team_a'),
            'opponent_team_id': fixture.get('team_h'),
            'was_home': False,
            'fixture_difficulty': fixture.get('team_a_difficulty', 0),
            'kickoff_time': fixture.get('kickoff_time'),
            'finished': fixture.get('finished', False),
            'team_a_score': fixture.get('team_a_score'),
            'team_h_score': fixture.get('team_h_score'),
        }
        
        # Add team names
        if fixture.get('team_a') in teams_lookup:
            record['team_name'] = teams_lookup[fixture['team_a']]['name']
        if fixture.get('team_h') in teams_lookup:
            record['opponent_team_name'] = teams_lookup[fixture['team_h']]['name']
        
        records.append(record)
        
        # Add reverse fixture (home team perspective)
        record_home = {
            'match_id': fixture.get('id'),
            'season': season,
            'gameweek': fixture.get('event'),
            'team_id': fixture.get('team_h'),
            'opponent_team_id': fixture.get('team_a'),
            'was_home': True,
            'fixture_difficulty': fixture.get('team_h_difficulty', 0),
            'kickoff_time': fixture.get('kickoff_time'),
            'finished': fixture.get('finished', False),
            'team_a_score': fixture.get('team_a_score'),
            'team_h_score': fixture.get('team_h_score'),
        }
        
        if fixture.get('team_h') in teams_lookup:
            record_home['team_name'] = teams_lookup[fixture['team_h']]['name']
        if fixture.get('team_a') in teams_lookup:
            record_home['opponent_team_name'] = teams_lookup[fixture['team_a']]['name']
        
        records.append(record_home)
    
    df = pd.DataFrame(records)
    
    # Calculate days of rest (requires sorting and time parsing)
    if 'kickoff_time' in df.columns and not df.empty:
        df['kickoff_time'] = pd.to_datetime(df['kickoff_time'], errors='coerce')
        df = df.sort_values(['team_id', 'gameweek', 'kickoff_time'])
        df['days_rest'] = df.groupby('team_id')['kickoff_time'].diff().dt.total_seconds() / (24 * 3600)
        df['days_rest'] = df['days_rest'].fillna(0)
    
    # Identify blank and double gameweeks
    if not df.empty:
        gw_counts = df.groupby(['team_id', 'gameweek']).size()
        df['is_double_gw'] = df.apply(
            lambda row: gw_counts.get((row['team_id'], row['gameweek']), 0) > 1,
            axis=1
        )
    
    return df


def create_team_match_stats(
    player_match_stats: pd.DataFrame,
    fixtures: pd.DataFrame,
    bootstrap_data: Dict
) -> pd.DataFrame:
    """
    Create team_match_stats table from aggregated player data.
    
    This table contains team-level match metrics:
    - xG, xGA (if available)
    - goals scored / conceded
    - clean sheet indicator
    
    Args:
        player_match_stats: DataFrame from create_player_match_stats_raw
        fixtures: DataFrame from create_fixtures_table
        bootstrap_data: Bootstrap static data
        
    Returns:
        DataFrame with team-level match statistics
    """
    if player_match_stats.empty:
        return pd.DataFrame()
    
    # Aggregate player stats to team level
    team_stats = player_match_stats.groupby([
        'team_id', 'season', 'gameweek', 'match_id', 'opponent_team_id', 'was_home'
    ]).agg({
        'goals_scored': 'sum',
        'assists': 'sum',
        'clean_sheets': 'max',  # If any player got CS, team got CS
        'goals_conceded': 'max',  # Same for all players on team
        'yellow_cards': 'sum',
        'red_cards': 'sum',
        'total_points': 'sum',
        'bps': 'sum',
    }).reset_index()
    
    # Merge with fixtures for opponent info
    if not fixtures.empty:
        team_stats = team_stats.merge(
            fixtures[['match_id', 'team_id', 'opponent_team_id', 'was_home', 
                     'team_a_score', 'team_h_score']],
            on=['match_id', 'team_id', 'opponent_team_id', 'was_home'],
            how='left',
            suffixes=('', '_fixture')
        )
        
        # Use actual goals from fixture if available
        team_stats['goals_scored'] = team_stats.apply(
            lambda row: row['team_h_score'] if row['was_home'] else row['team_a_score']
            if pd.notna(row.get('team_h_score')) else row['goals_scored'],
            axis=1
        )
        team_stats['goals_conceded'] = team_stats.apply(
            lambda row: row['team_a_score'] if row['was_home'] else row['team_h_score']
            if pd.notna(row.get('team_a_score')) else row['goals_conceded'],
            axis=1
        )
    
    # Rename columns for clarity
    team_stats = team_stats.rename(columns={
        'goals_scored': 'team_goals',
        'goals_conceded': 'team_goals_conceded',
        'clean_sheets': 'team_clean_sheet',
    })
    
    # Add team names
    teams_lookup = {t['id']: t['name'] for t in bootstrap_data['teams']}
    team_stats['team_name'] = team_stats['team_id'].map(teams_lookup)
    
    opponent_lookup = {t['id']: t['name'] for t in bootstrap_data['teams']}
    team_stats['opponent_team_name'] = team_stats['opponent_team_id'].map(opponent_lookup)
    
    return team_stats


def save_dataframe(df: pd.DataFrame, filepath: Path, format: str = 'parquet') -> None:
    """
    Save DataFrame to file.
    
    Args:
        df: DataFrame to save
        filepath: Path to save file
        format: File format ('parquet', 'csv', or 'json')
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    if format == 'parquet':
        df.to_parquet(filepath, index=False, engine='pyarrow')
    elif format == 'csv':
        df.to_csv(filepath, index=False)
    elif format == 'json':
        df.to_json(filepath, orient='records', indent=2)
    else:
        raise ValueError(f"Unsupported format: {format}")
