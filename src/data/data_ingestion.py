"""
Main data ingestion pipeline for FPL data.

This script orchestrates the complete data ingestion process:
1. Fetch data from FPL API
2. Transform raw data into structured tables
3. Save data to data/raw/ directory

Usage:
    python -m src.data.data_ingestion --season 2024-25 --max-players 100
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.data.fpl_api_client import FPLAPIClient, save_json
from src.data.data_transformers import (
    create_player_match_stats_raw,
    create_fixtures_table,
    create_team_match_stats,
    save_dataframe,
)


logger = logging.getLogger(__name__)


def get_current_season() -> str:
    """Get current season string based on current date."""
    now = datetime.now()
    year = now.year
    month = now.month
    
    # FPL seasons typically start in August
    if month >= 8:
        return f"{year}-{year+1}"
    else:
        return f"{year-1}-{year}"


def setup_logging(log_dir: Path, season: str) -> Path:
    """
    Configure logging to write both to console and a file in the logs directory.

    Parameters
    ----------
    log_dir : Path
        Directory where log files will be stored.
    season : str
        Season identifier to include in the log filename.

    Returns
    -------
    Path
        Path to the log file being written to.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"data_ingestion_{season}_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logger.info("Logging initialised. Log file: %s", log_file)
    return log_file


def run_ingestion_pipeline(
    season: str,
    data_dir: Path,
    max_players: Optional[int] = None,
    fetch_all_players: bool = False
) -> None:
    """
    Run the complete data ingestion pipeline.

    Parameters
    ----------
    season : str
        Season identifier (e.g., '2024-25').
    data_dir : Path
        Base directory for data storage.
    max_players : Optional[int]
        Optional limit on number of players to fetch (for testing).
    fetch_all_players : bool
        If True, fetch detailed data for all players (slow!).
    """
    logger.info("Starting data ingestion pipeline for season %s", season)
    logger.info("Data will be saved to: %s", data_dir)

    # Initialize API client
    client = FPLAPIClient()

    # Step 1: Fetch bootstrap-static data
    logger.info("[1/4] Fetching bootstrap-static data...")
    bootstrap_data = client.get_bootstrap_static()
    bootstrap_path = data_dir / "raw" / f"bootstrap-static_{season}.json"
    save_json(bootstrap_data, bootstrap_path)
    logger.info("Saved bootstrap-static data to %s", bootstrap_path)

    # Extract basic info
    num_players = len(bootstrap_data['elements'])
    num_teams = len(bootstrap_data['teams'])
    num_events = len(bootstrap_data["events"])
    logger.info(
        "Found %d players, %d teams, %d gameweeks",
        num_players,
        num_teams,
        num_events,
    )

    # Step 2: Fetch fixtures data
    logger.info("[2/4] Fetching fixtures data...")
    fixtures_data = client.get_fixtures()
    fixtures_path = data_dir / "raw" / f"fixtures_{season}.json"
    save_json(fixtures_data, fixtures_path)
    logger.info("Saved fixtures data to %s", fixtures_path)
    logger.info("Found %d fixtures", len(fixtures_data))

    # Step 3: Fetch player data (optional, can be slow)
    all_players_data = []
    if fetch_all_players:
        logger.info("[3/4] Fetching detailed data for all players...")
        logger.info("This may take several minutes...")
        all_players_data = client.get_all_players_data(max_players=max_players)
        players_data_path = data_dir / "raw" / f"all_players_data_{season}.json"
        save_json({"players": all_players_data}, players_data_path)
        logger.info("Saved player data to %s", players_data_path)
        logger.info("Fetched data for %d players", len(all_players_data))
    else:
        logger.info(
            "[3/4] Skipping detailed player data fetch (use --fetch-all-players to enable)"
        )
        logger.info(
            "Note: You can fetch this later or use historical data files"
        )

    # Step 4: Transform and save structured tables
    logger.info("[4/4] Transforming data into structured tables...")

    # Create fixtures table
    if fixtures_data:
        fixtures_df = create_fixtures_table(fixtures_data, bootstrap_data, season)
        fixtures_output_path = data_dir / "raw" / f"fixtures_{season}.parquet"
        save_dataframe(fixtures_df, fixtures_output_path, format="parquet")
        logger.info(
            "Created fixtures table with %d rows, saved to %s",
            len(fixtures_df),
            fixtures_output_path,
        )

    # Create player_match_stats_raw table (if player data available)
    if all_players_data:
        player_stats_df = create_player_match_stats_raw(
            bootstrap_data, all_players_data, season
        )
        player_stats_path = (
            data_dir / "raw" / f"player_match_stats_raw_{season}.parquet"
        )
        save_dataframe(player_stats_df, player_stats_path, format="parquet")
        logger.info(
            "Created player_match_stats_raw table with %d rows, saved to %s",
            len(player_stats_df),
            player_stats_path,
        )

        # Create team_match_stats table
        if not fixtures_df.empty:
            team_stats_df = create_team_match_stats(
                player_stats_df, fixtures_df, bootstrap_data
            )
            team_stats_path = (
                data_dir / "raw" / f"team_match_stats_{season}.parquet"
            )
            save_dataframe(team_stats_df, team_stats_path, format="parquet")
            logger.info(
                "Created team_match_stats table with %d rows, saved to %s",
                len(team_stats_df),
                team_stats_path,
            )
    else:
        logger.warning("Skipping player_match_stats_raw (no player data fetched)")
        logger.warning("Skipping team_match_stats (requires player data)")

    logger.info("=" * 60)
    logger.info("Data ingestion pipeline completed successfully!")
    logger.info("=" * 60)
    logger.info("Next steps:")
    logger.info("1. Review the data in data/raw/")
    logger.info("2. Run feature engineering to create player_gw_features table")
    logger.info("3. Proceed with modeling")


def main():
    """Main entry point for data ingestion pipeline."""
    parser = argparse.ArgumentParser(
        description="FPL Data Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch bootstrap and fixtures only (fast)
  python -m src.data.data_ingestion --season 2024-25
  
  # Fetch all data including player history (slow, ~10-30 minutes)
  python -m src.data.data_ingestion --season 2024-25 --fetch-all-players
  
  # Test with limited players
  python -m src.data.data_ingestion --season 2024-25 --fetch-all-players --max-players 50
        """
    )
    
    parser.add_argument(
        '--season',
        type=str,
        default=None,
        help='Season identifier (e.g., 2024-25). Defaults to current season.'
    )
    
    parser.add_argument(
        '--data-dir',
        type=str,
        default=None,
        help='Base directory for data storage. Defaults to project_root/data/'
    )
    
    parser.add_argument(
        '--fetch-all-players',
        action='store_true',
        help='Fetch detailed data for all players (slow, makes many API calls)'
    )
    
    parser.add_argument(
        '--max-players',
        type=int,
        default=None,
        help='Limit number of players to fetch (for testing)'
    )
    
    args = parser.parse_args()

    # Set defaults
    if args.season is None:
        args.season = get_current_season()

    if args.data_dir is None:
        args.data_dir = project_root / "data"
    else:
        args.data_dir = Path(args.data_dir)

    # Set up logging
    logs_dir = project_root / "logs"
    log_file = setup_logging(logs_dir, args.season)
    logger.info("Log file: %s", log_file)

    # Run pipeline
    try:
        run_ingestion_pipeline(
            season=args.season,
            data_dir=args.data_dir,
            max_players=args.max_players,
            fetch_all_players=args.fetch_all_players
        )
    except Exception as e:
        print(f"\n❌ Error during data ingestion: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()