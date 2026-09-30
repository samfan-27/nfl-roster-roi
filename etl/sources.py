"""nflverse extraction and schema checks. Domain calculations live in src."""

from pathlib import Path
import nflreadpy as nfl
from nflreadpy.config import update_config


class SourceNotReadyError(RuntimeError):
    """Required source release has not been published yet."""


def configure_sources(fresh=False):
    update_config(
        cache_mode="filesystem", cache_dir=Path(".cache/nflreadpy"),
        cache_duration=0 if fresh else 3600, verbose=False,
    )


def _load(loader, *args):
    return loader(*args).to_pandas()


def load_reference_tables():
    return {"players": _load(nfl.load_players), "contracts": _load(nfl.load_contracts)}


def load_season_tables(season):
    """Load weekly data so regular-season EPA and snaps share game coverage."""
    try:
        tables = {
            "player_stats": _load(nfl.load_player_stats, season),
            "snap_counts": _load(nfl.load_snap_counts, season),
            "rosters": _load(nfl.load_rosters, season),
        }
    except ConnectionError as exc:
        response = getattr(exc.__cause__, "response", None)
        if response is not None and response.status_code == 404:
            raise SourceNotReadyError(f"Season {season} source release is not available") from exc
        raise
    requirements = {
        "player_stats": {"player_id", "season", "week", "season_type", "game_id", "team", "passing_epa", "rushing_epa", "receiving_epa"},
        "snap_counts": {"pfr_player_id", "season", "game_type", "game_id", "week", "offense_snaps", "defense_snaps"},
        "rosters": {"gsis_id", "team", "position", "years_exp", "birth_date"},
    }
    for name, required in requirements.items():
        missing = required - set(tables[name].columns)
        if missing:
            raise ValueError(f"{name} is missing required columns: {sorted(missing)}")
    return tables


def current_season():
    return nfl.get_current_season()
