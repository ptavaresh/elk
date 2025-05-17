import argparse
from dfscanner_class import DFScanner
from colorama import Fore, Style


def main():
    parser = argparse.ArgumentParser(description="Dask DFScanner")
    parser.add_argument("-file", type=str, required=True, help="Ruta al archivo CSV")
    parser.add_argument("-scan", action="store_true", help="Escanear valores con regex")
    parser.add_argument("-apply_regex", action="store_true", help="Aplicar regex a columnas")
    parser.add_argument("-report", action="store_true", help="Mostrar reporte de cambios")
    args = parser.parse_args()

    scanner = DFScanner(args.file)

    if args.scan:
        found = scanner.scan_values()
        msg = "Se encontraron coincidencias." if found else "No se encontraron coincidencias."
        print(Fore.YELLOW + msg)

    if args.apply_regex:
        scanner.apply_regex_to_columns()
        print(Fore.YELLOW + f"Conteo de coincidencias por columna: {scanner.regex_counters}")

    if args.report:
        report_df = scanner.get_report_df()
        if not report_df.empty:
            print(Fore.YELLOW + "\n=== Reporte de coincidencias ===\n")
            print(report_df.to_string(index=False))
        else:
            print(Fore.CYAN + "No se encontraron coincidencias para reportar.")


if __name__ == "__main__":
    main()
