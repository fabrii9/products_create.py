#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import base64
import xmlrpc.client
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading

# =========================
# CONFIGURACIÓN ODOO
# =========================

ODOO_URL = os.getenv("ODOO_URL", "https://odoo.com")
ODOO_DB = os.getenv("ODOO_DB", "Testing")
ODOO_USER = os.getenv("ODOO_USER", "admin")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "admin")

# Thread local: un cliente/importer por hilo
_thread_local = threading.local()


# =========================
# HELPERS GENERALES
# =========================

def _to_bool(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "t", "si", "sí", "yes", "y", "x", "s")


def _to_float(v: Any) -> float:
    if v is None or v == "":
        return 0.0
    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return 0.0


def _safe_str(v: Any) -> str:
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return "" if v is None else str(v).strip()


# =========================
# CLIENTE ODOO (XML-RPC)
# =========================

class OdooClient:
    def __init__(self, url: str, db: str, user: str, password: str):
        self.url = url.rstrip("/")
        self.db = db
        self.user = user
        self.password = password

        common = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/common")
        self.uid = common.authenticate(self.db, self.user, self.password, {})
        if not self.uid:
            raise RuntimeError("Error autenticando en Odoo.")

        self.models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object")

    def search(self, model: str, domain: List, limit: int = 0) -> List[int]:
        return self.models.execute_kw(
            self.db, self.uid, self.password, model, "search", [domain],
            {"limit": limit} if limit else {},
        )

    def create(self, model: str, vals: Dict) -> int:
        return self.models.execute_kw(
            self.db, self.uid, self.password, model, "create", [vals]
        )

    def write(self, model: str, ids: List[int], vals: Dict) -> bool:
        return self.models.execute_kw(
            self.db, self.uid, self.password, model, "write", [ids, vals]
        )

    def read(self, model: str, ids: List[int], fields: List[str]):
        return self.models.execute_kw(
            self.db, self.uid, self.password, model, "read", [ids, fields]
        )


# =========================
# IMPORTADOR DE PRODUCTOS
# =========================

class ProductImporter:
    def __init__(self, client: OdooClient, base_dir: str = "."):
        self.client = client
        self.base_dir = base_dir

    # ---------- CATEGORÍA ----------
    def ensure_category(self, value: str) -> Optional[int]:
        """
        Emula comportamiento de 'categ_id/id':
        - ID numérico
        - module.external_id
        - external_id solo
        - nombre de categoría
        """
        value = _safe_str(value)
        if not value:
            return None

        # 1) ID numérico
        if value.isdigit():
            ids = self.client.search(
                "product.category", [("id", "=", int(value))], limit=1
            )
            if ids:
                return ids[0]

        # 2) XML-ID completo module.name
        if "." in value:
            module, name = value.split(".", 1)
            domain = [
                ("module", "=", module),
                ("name", "=", name),
                ("model", "=", "product.category"),
            ]
            ids = self.client.search("ir.model.data", domain, limit=1)
            if ids:
                rec = self.client.read("ir.model.data", ids, ["res_id"])
                if rec and rec[0].get("res_id"):
                    return rec[0]["res_id"]

        # 3) Solo external_id
        ids = self.client.search(
            "ir.model.data",
            [("name", "=", value), ("model", "=", "product.category")],
            limit=1,
        )
        if ids:
            rec = self.client.read("ir.model.data", ids, ["res_id"])
            if rec and rec[0].get("res_id"):
                return rec[0]["res_id"]

        # 4) Nombre de categoría
        ids = self.client.search(
            "product.category", [("name", "=", value)], limit=1
        )
        if ids:
            return ids[0]

        print(f"[WARN] Categoría no encontrada: '{value}'. Usará 'All'.")
        return None

    # -------------------------------------------------------------

    def import_row(self, row: pd.Series) -> str:
        """
        Importa/actualiza un solo product.template a partir de una fila.
        Columnas soportadas:
        - default_code
        - name
        - categ_id/id  (o 'categoria de producto / external id')
        - supplier_code
        - standard_price
        - brand
        - barcode
        - list_price
        - available_in_pos
        - purchase_ok
        - sale_ok
        - is_storable
        """

        default_code = _safe_str(row.get("default_code"))
        name = _safe_str(row.get("name"))
        if not name and not default_code:
            return "(sin nombre ni código)"

        # --- CATEGORÍA: soportar ambos nombres de columna ---
        if "categ_id/id" in row:
            category_value = _safe_str(row.get("categ_id/id"))
        else:
            category_value = _safe_str(row.get("categoria de producto / external id"))

        categ_id = self.ensure_category(category_value)

        supplier_code = _safe_str(row.get("supplier_code"))
        brand_name = _safe_str(row.get("brand"))

        barcode = _safe_str(row.get("barcode"))
        if barcode.lower() == "nan":
            barcode = ""

        list_price = _to_float(row.get("list_price"))
        standard_price = _to_float(row.get("standard_price"))

        available_in_pos = _to_bool(row.get("available_in_pos"))
        purchase_ok = _to_bool(row.get("purchase_ok"))
        sale_ok = _to_bool(row.get("sale_ok"))

        # campo boolean custom
        is_storable_flag = _to_bool(row.get("is_storable"))

        # --- armamos vals ---
        vals: Dict[str, Any] = {}

        if name:
            vals["name"] = name
        if default_code:
            vals["default_code"] = default_code
        if barcode:
            vals["barcode"] = barcode
        if list_price:
            vals["list_price"] = list_price
        if standard_price:
            vals["standard_price"] = standard_price

        if supplier_code:
            vals["supplier_code"] = supplier_code

        if brand_name:
            vals["brand"] = brand_name

        vals["available_in_pos"] = bool(available_in_pos)
        vals["purchase_ok"] = bool(purchase_ok)
        vals["sale_ok"] = bool(sale_ok)

        # campo custom booleano is_storable
        vals["is_storable"] = bool(is_storable_flag)

        if categ_id:
            vals["categ_id"] = categ_id

        # --- Buscar producto ---
        if default_code:
            search = [("default_code", "=", default_code)]
        else:
            search = [("name", "=", name)]

        ids = self.client.search("product.template", search, limit=1)

        if ids:
            self.client.write("product.template", [ids[0]], vals)
            return f"update: {default_code or name}"
        else:
            self.client.create("product.template", vals)
            return f"create: {default_code or name}"


# =========================
# THREAD-LOCAL IMPORTER
# =========================

def get_thread_importer(base_dir: str) -> ProductImporter:
    """
    Crea un OdooClient + ProductImporter por hilo y los reutiliza.
    """
    if not hasattr(_thread_local, "importer"):
        client = OdooClient(ODOO_URL, ODOO_DB, ODOO_USER, ODOO_PASSWORD)
        _thread_local.importer = ProductImporter(client, base_dir=base_dir)
    return _thread_local.importer


def worker_task(
    args: Tuple[int, Dict[str, Any], str, bool]
) -> Tuple[int, bool, str]:
    """
    Función que ejecuta cada hilo.
    Devuelve: (row_index, ok, mensaje)
    """
    row_index, row_dict, base_dir, dry_run = args
    row = pd.Series(row_dict)

    try:
        if dry_run:
            return row_index, True, "(dry-run)"

        importer = get_thread_importer(base_dir)
        label = importer.import_row(row)
        return row_index, True, label

    except Exception as e:
        dc = _safe_str(row_dict.get("default_code"))
        name = _safe_str(row_dict.get("name"))
        msg = f"ERROR en {dc or name or '(sin identificador)'}: {e}"
        return row_index, False, msg


# =========================
# MAIN
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Importar productos desde un Excel a Odoo (product.template) con hilos."
    )
    parser.add_argument("--file", "-f", required=True, help="Ruta al archivo Excel")
    parser.add_argument("--sheet-name", help="Nombre de la hoja (opcional)")
    parser.add_argument("--dry-run", action="store_true", help="No escribe en Odoo")
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Cantidad de hilos (default: 4)",
    )
    args = parser.parse_args()

    excel_path = args.file
    if not os.path.exists(excel_path):
        print(f"ERROR: no se encontró el archivo: {excel_path}")
        sys.exit(1)

    # Leer Excel
    try:
        if args.sheet_name:
            df = pd.read_excel(excel_path, sheet_name=args.sheet_name)
        else:
            df = pd.read_excel(excel_path)
    except Exception as e:
        print(f"ERROR al leer el Excel: {e}")
        sys.exit(1)

    df.columns = [str(c).strip() for c in df.columns]
    total = len(df)
    if total == 0:
        print("No hay filas para procesar.")
        sys.exit(0)

    base_dir = os.path.dirname(os.path.abspath(excel_path))

    print(
        f"Conectando a Odoo en {ODOO_URL} DB={ODOO_DB} usuario={ODOO_USER} "
        f"(workers={args.workers})..."
    )

    records = df.to_dict("records")
    tasks = [
        (idx, records[idx], base_dir, args.dry_run)
        for idx in range(total)
    ]

    ok_count = 0
    err_count = 0

    print(f"Filas a procesar: {total}")
    print("Inicio de importación...\n")

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # executor.map preserva el orden de las filas originales
        for processed, result in enumerate(executor.map(worker_task, tasks), start=1):
            row_index, ok, msg = result
            row_num = row_index + 1
            if ok:
                ok_count += 1
                print(f"[{row_num}/{total}] OK -> {msg}")
            else:
                err_count += 1
                print(f"[{row_num}/{total}] {msg}")

    print("\n============================")
    print("FIN DE IMPORTACIÓN")
    print(f"Correctos: {ok_count}")
    print(f"Errores:   {err_count}")
    print("============================")


if __name__ == "__main__":
    main()
