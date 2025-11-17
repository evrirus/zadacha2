import csv
import sys
import tarfile
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, List
from urllib.request import urlopen

"""
Примеры config


package_name,repo_url_or_path,repo_mode,ascii_tree,filter_substring
busybox,https://dl-cdn.alpinelinux.org/alpine/v3.18/main/x86_64/,remote,on,busy

package_name,repo_url_or_path,repo_mode,ascii_tree,filter_substring
busybox,{путь до директории где находится архив tar.gz},local,on,busy

package_name - имя пакета
repo_url_or_path - урл или путь
repo_mode - как обрабатывать, путь или url
ascii_tree - Режим вывода зависимостей
filter_substring - то, что нужно исключить(из названия)
"""

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
    pass

class ApkIndexError(Exception):
    pass

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

    if "filter_substring" not in cfg:
        errors.append("filter_substring должен присутствовать (пустая строка допустима).")

    if errors:
        raise ConfigError("\n".join(errors))

        # Если пустая строка — оставляем так
    cfg.setdefault("filter_substring", "")

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
    """
    Локальный режим: путь к конкретному файлу.
    """
    if not path.exists():
        raise ApkIndexError(f"Файл не найден: {path}")

    # Проверяем, является ли файл tar.gz
    if path.suffixes == ['.tar', '.gz'] or path.name.lower() == "apkindex.tar.gz":
        return path.read_bytes()

    # Если это обычный APKINDEX
    if path.name.upper() == "APKINDEX":
        return path.read_bytes()

    raise ApkIndexError(
        f"Файл {path} не является APKINDEX.tar.gz или APKINDEX.\n"
        f"Укажите путь к реальному файлу индекса."
    )


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



# этап 3
def build_graph(records: List[Dict[str, str]], filter_substr: str) -> Dict[str, List[str]]:
    graph = {}
    for rec in records:
        pkg = rec.get("P")
        if not pkg:
            continue
        # только если filter_substr непустой
        if filter_substr and filter_substr in pkg:
            continue

        dep_field = rec.get("D", "")
        # фильтруем зависимости аналогично
        deps = [d for d in dep_field.split() if not filter_substr or filter_substr not in d]

        graph[pkg] = deps
    return graph


def build_graph_from_testfile(path: Path, filter_substr: str) -> Dict[str, List[str]]:
    graph = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        pkg, deps_str = line.split(":", 1)
        pkg = pkg.strip()
        deps = [d.strip() for d in deps_str.split() if filter_substr not in d]
        if filter_substr not in pkg:
            graph[pkg] = deps
    return graph

def dfs_transitive(graph: Dict[str, List[str]], start_pkg: str) -> List[str]:
    visited = set()
    stack = [start_pkg]
    result = []

    while stack:
        pkg = stack.pop()
        if pkg in visited:
            continue
        visited.add(pkg)
        result.append(pkg)
        for dep in graph.get(pkg, []):
            if dep not in visited:
                stack.append(dep)
    result.remove(start_pkg)  # убрать сам пакет из списка зависимостей
    return result

def print_graph(graph: Dict[str, List[str]]):
    print("\n=== Граф зависимостей ===")
    for pkg, deps in graph.items():
        print(f"{pkg}: {', '.join(deps) if deps else '(нет зависимостей)'}")

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


        # этап 3
        filter_substr = cfg["filter_substring"]

        if cfg["repo_mode"] == "local":
            raw = fetch_index_local(Path(repo))
            text = extract_index(raw)
            records = parse_apkindex(text)
            graph = build_graph(records, filter_substr)
        else:
            raw = fetch_index_remote(repo)
            text = extract_index(raw)
            records = parse_apkindex(text)
            graph = build_graph(records, filter_substr)

        print_graph(graph)

        # Пример: получить все зависимости для пакета
        trans_deps = dfs_transitive(graph, pkg)
        print(f"\nТранзитивные зависимости пакета '{pkg}':")
        if trans_deps:
            for d in trans_deps:
                print(f" - {d}")
        else:
            print(" (нет транзитивных зависимостей)")

    except ConfigError as e:
        print("Ошибка конфигурации:")
        print(e)
        sys.exit(2)

if __name__ == "__main__":
    main()
