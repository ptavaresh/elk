import dask.dataframe as dd
import pandas as pd
import re
import yaml
from tqdm import tqdm
from colorama import init, Fore, Style
import time


class DFScanner:
    def __init__(self, csv_path: str, config_path: str = "config.yml"):
        init(autoreset=True)
        self.df = dd.read_csv(csv_path, dtype=str)
        self.patterns = self.load_patterns(config_path)
        self.regex_counters = {}
        self.report = []

    def load_patterns(self, path: str) -> list:
        with open(path, "r") as f:
            config = yaml.safe_load(f)
        return [re.compile(p) for p in config.get("regex_patterns", [])]

    def apply_regex_to_columns(self):
        print(Fore.BLUE + "\n[+] Iniciando análisis por columnas...\n" + Style.RESET_ALL)
        start_time = time.time()

        for column in tqdm(self.df.columns, desc="Escaneando columnas"):
            for pattern in self.patterns:
                try:
                    if isinstance(pattern, str):
                        regex = re.compile(pattern, flags=re.IGNORECASE)
                    else:
                        regex = pattern

                    match = self.df[column].astype(str).str.contains(regex, regex=True, na=False)
                    if match.any().compute():
                        self.report[column].append(pattern)
                except Exception as e:
                    print(Fore.RED + f"[-] Error procesando columna '{column}': {e}" + Style.RESET_ALL)

        duration = time.time() - start_time
        print(Fore.GREEN + f"\n[✓] Análisis completado en {duration:.2f} segundos.\n" + Style.RESET_ALL)

    def scan_values(self) -> bool:
        print(Fore.BLUE + "\n[+] Iniciando escaneo de valores del DataFrame...\n")
        start_time = time.time()

        def row_matches(row, patterns):
            for val in row:
                for pattern in patterns:
                    if pattern.search(str(val)):
                        return True
            return False

        found_any = False
        # Iterar partición por partición con tqdm para mostrar progreso real
        for partition in tqdm(self.df.partitions, desc="Escaneando particiones"):
            result = partition.map_partitions(
                lambda df: df.apply(row_matches, axis=1, args=(self.patterns,))
            ).compute()

            if result.any():
                found_any = True
                break

        elapsed = time.time() - start_time
        print(Fore.GREEN + f"\n[✓] Escaneo terminado en {elapsed:.2f} segundos")
        return found_any


    def get_report_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.report)
