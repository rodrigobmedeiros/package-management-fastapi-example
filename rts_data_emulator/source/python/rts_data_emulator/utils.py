from pathlib import Path


def get_data_directory(current_dir: Path, levels_above: int) -> Path:
    n_parents = 0
    parent_dir = current_dir
    while n_parents < levels_above:
        parent_dir = parent_dir.parent
        n_parents += 1
    return parent_dir / "src" / "data"
