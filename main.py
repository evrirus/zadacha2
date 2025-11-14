import csv
import sys
from pathlib import Path
from typing import Dict, Any


REQUIRED_KEYS = {
    "package_name",
    "repo_url_or_path",
    "repo_mode",
    "ascii_tree",
    "filter_substring"
}

VALID_REPO_MODES = {"local", "remote"}
VALID_ASCII_TREE_MODES = {"on", "off"}


class ConfigError(Exception):
    """Custom exception for configuration problems."""


def load_config(csv_path: str) -> Dict[str, Any]:
    path = Path(csv_path)

    if not path.exists():
        raise ConfigError(f"Файл конфигурации не найден: {path}")

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        raise ConfigError("Файл конфигурации пуст.")

    if len(rows) > 1:
        raise ConfigError("Конфигурационный файл должен содержать только одну строку.")

    config = rows[0]

    missing = REQUIRED_KEYS - set(config.keys())
    if missing:
        raise ConfigError(f"Отсутствуют обязательные поля: {', '.join(missing)}")

    return config


def validate_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    errors = []

    # Имя пакета
    if not cfg["package_name"]:
        errors.append("Имя пакета не может быть пустым.")

    # URL или путь
    if not cfg["repo_url_or_path"]:
        errors.append("URL или путь к репозиторию должны быть указаны.")
    else:
        # Допустимо: путь или URL (простейшая проверка)
        val = cfg["repo_url_or_path"]
        if not (val.startswith("http://") or val.startswith("https://") or Path(val).exists()):
            errors.append("repo_url_or_path должен быть существующим путём или URL.")

    # Режим
    if cfg["repo_mode"] not in VALID_REPO_MODES:
        errors.append(f"repo_mode должен быть одним из {VALID_REPO_MODES}")

    # ASCII дерево
    if cfg["ascii_tree"] not in VALID_ASCII_TREE_MODES:
        errors.append(f"ascii_tree должен быть одним из {VALID_ASCII_TREE_MODES}")

    # Фильтр
    if cfg["filter_substring"] is None:
        errors.append("filter_substring не должен быть пустым (можно оставить пустую строку).")

    if errors:
        raise ConfigError("\n".join(errors))

    return cfg


def print_config(cfg: Dict[str, Any]) -> None:
    for k, v in cfg.items():
        print(f"{k}: {v}")


def main():
    if len(sys.argv) != 2:
        print("Использование: python app.py path/to/config.csv")
        sys.exit(1)

    try:
        cfg = load_config(sys.argv[1])
        cfg = validate_config(cfg)
        print_config(cfg)

    except ConfigError as e:
        print("Ошибка конфигурации:")
        print(e)
        sys.exit(2)


if __name__ == "__main__":
    main()
