import csv
import sys
import tarfile
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, List
from urllib.request import urlopen

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

class ApkIndexError(Exception):
    """Custom exception for APK problems."""

def is_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")



def load_config(csv_path: str) -> Dict[str, Any]:
    path = Path(csv_path)

    if not path.exists():
        raise ConfigError(f"Конфигурации не найден: {path}")

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        raise ConfigError("Конфиг пуст.")

    if len(rows) > 1:
        raise ConfigError("Конфиг должен содержать только одну строку.")

    config = rows[0]

    missing = REQUIRED_KEYS - set(config.keys())
    if missing:
        raise ConfigError(f"Отсутствуют обязательные поля: {', '.join(missing)}")

    return config


def validate_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    errors = []

    if not cfg["package_name"]:
        errors.append("Имя пакета не может быть пустым.")

    if not cfg["repo_url_or_path"]:
        errors.append("URL или путь к репозиторию должны быть указаны.")
    else:
        # Допустимо: путь или URL
        val = cfg["repo_url_or_path"]
        if not (val.startswith("http://") or val.startswith("https://") or Path(val).exists()):
            errors.append("repo_url_or_path должен быть существующим путём или URL.")

    if cfg["repo_mode"] not in VALID_REPO_MODES:
        errors.append(f"repo_mode должен быть одним из {VALID_REPO_MODES}")

    if cfg["ascii_tree"] not in VALID_ASCII_TREE_MODES:
        errors.append(f"ascii_tree должен быть одним из {VALID_ASCII_TREE_MODES}")

    if cfg["filter_substring"] is None:
        errors.append("filter_substring не должен быть пустым (можно оставить пустую строку).")

    if errors:
        raise ConfigError("\n".join(errors))

    return cfg



# этап 2
def fetch_index_remote(repo_url: str) -> bytes:
    if not repo_url.endswith("/"):
        repo_url += "/"

    candidates = [
        repo_url + "APKINDEX.tar.gz",
        repo_url + "APKINDEX"
    ]

    last_error = None

    for url in candidates:
        try:

            with urlopen(url, timeout=20) as r:
                data = r.read()
                if data:
                    return data
        except Exception as e:
            last_error = e

    raise ApkIndexError(f"Не удалось загрузить APKINDEX с {repo_url} ({last_error})")

def fetch_index_local(path: Path) -> bytes:
    p1 = path / "APKINDEX.tar.gz"
    p2 = path / "APKINDEX"

    if p1.exists():
        return p1.read_bytes()
    if p2.exists():
        return p2.read_bytes()

    raise ApkIndexError(f"В каталоге {path} нет APKINDEX.tar.gz или APKINDEX")

def extract_index(raw: bytes) -> str:
    try:
        bio = BytesIO(raw)

        # открыть архив без записи на диск
        with tarfile.open(fileobj=bio, mode="r:gz") as tf:
            for m in tf.getmembers():
                if m.name.endswith("APKINDEX") or m.name == "APKINDEX":
                    f = tf.extractfile(m)
                    if f is None:
                        raise ApkIndexError("Не удалось извлечь APKINDEX из архива.")
                    return f.read().decode("utf-8", errors="replace")

        raise ApkIndexError("APKINDEX не найден внутри архива.")
    except tarfile.ReadError:
        return raw.decode("utf-8", errors="replace")

def parse_apkindex(text: str) -> List[Dict[str, str]]:
    blocks = text.strip().split("\n\n")
    records = []

    for block in blocks:
        lines = block.strip().splitlines()
        rec = {}
        for ln in lines:
            if ":" not in ln:
                continue
            k, v = ln.split(":", 1)
            rec[k] = v.strip()
        if rec:
            records.append(rec)

    return records

def get_package_dependencies(records: List[Dict[str, str]], package: str) -> List[str]:
    matches = [r for r in records if r.get("P") == package]
    if not matches:
        raise ApkIndexError(f"Пакет '{package}' не найден.")

    deps_raw = matches[0].get("D")
    if not deps_raw:
        return []

    return [d for d in deps_raw.split() if d]



def main():
    if len(sys.argv) != 2:
        print("Использование: python app.py path/to/config.csv")
        sys.exit(1)

    try:
        cfg = load_config(sys.argv[1])
        cfg = validate_config(cfg)

        # 2 этап
        repo = cfg["repo_url_or_path"]
        pkg = cfg["package_name"]

        print("=== Получение зависимостей ===")
        if cfg["repo_mode"] == "remote":
            raw = fetch_index_remote(repo)
        else:
            raw = fetch_index_local(Path(repo))

        text = extract_index(raw)
        records = parse_apkindex(text)
        deps = get_package_dependencies(records, pkg)

        if deps:
            print(f"Прямые зависимости пакета '{pkg}':")
            for d in deps:
                print(f" - {d}")
        else:
            print(f"У пакета '{pkg}' нет прямых зависимостей.")

    except ConfigError as e:
        print("Ошибка конфигурации:")
        print(e)
        sys.exit(2)

if __name__ == "__main__":
    main()
