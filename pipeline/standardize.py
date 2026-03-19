#!/usr/bin/env python3
"""Standardize mixed source files into DB target-table CSVs."""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

from openpyxl import load_workbook
import pdfplumber


CREATE_RE = re.compile(r"^\s*create\s+table\s+([A-Za-z0-9_]+)\s*$", re.IGNORECASE)
EPA_CODE_RE = re.compile(r"E\d+_I_\d+", re.IGNORECASE)
JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)
PDF_DATE_RE = re.compile(r"Date:\s*(.*?)\s+Shift:", re.IGNORECASE | re.DOTALL)
PDF_SHIFT_RE = re.compile(
    r"Shift:\s*(.*?)\s+Patient Information", re.IGNORECASE | re.DOTALL
)
PDF_PAT_RE = re.compile(r"Patient ID:\s*(.*?)\s+Case ID:", re.IGNORECASE | re.DOTALL)
PDF_CASE_RE = re.compile(r"Case ID:\s*(.*?)\s+Ward:", re.IGNORECASE | re.DOTALL)
PDF_WARD_RE = re.compile(r"Ward:\s*(.*?)\s+Report", re.IGNORECASE | re.DOTALL)
SQL_INSERT_RE = re.compile(
    r"insert\s+into\s+([^\s(]+)\s*(?:\((.*?)\))?\s*values\s*(.*?);",
    re.IGNORECASE | re.DOTALL,
)
TEXTY_COL_RE = re.compile(
    r"(text|note|report|comment|summary|desc|description|narrative|observ|befund|anamn|free)",
    re.IGNORECASE,
)


TABLE_HINTS = {
    "tbImportLabsData": ["lab", "labs"],
    "tbImportIcd10Data": ["icd", "ops"],
    "tbImportDevice1HzMotionData": ["1hz", "raw_1hz"],
    "tbImportDeviceMotionData": ["device", "motion", "fall"],
    "tbImportMedicationInpatientData": ["medication", "med"],
    "tbImportNursingDailyReportsData": ["nursing", "report"],
    "tbImportAcData": ["epaac", "epaac-data", "einsch"],
}

CONTENT_HINTS = {
    "tbImportLabsData": [
        "sodium",
        "potassium",
        "creatinine",
        "specimen",
        "glucose",
        "hemoglobin",
        "wbc",
        "platelets",
        "crp",
        "lactate",
    ],
    "tbImportIcd10Data": [
        "icd",
        "ops",
        "diagnosis",
        "procedure",
        "admission",
        "discharge",
    ],
    "tbImportDevice1HzMotionData": [
        "1hz",
        "accel",
        "pressure_zone",
        "bed_occupied",
        "movement_score",
    ],
    "tbImportDeviceMotionData": [
        "movement_index",
        "micro_movements",
        "bed_exit",
        "fall_event",
        "impact",
    ],
    "tbImportMedicationInpatientData": [
        "medication",
        "atc",
        "dose",
        "route",
        "prescriber",
        "administration",
    ],
    "tbImportNursingDailyReportsData": [
        "nursing",
        "shift",
        "ward",
        "report",
        "note",
    ],
    "tbImportAcData": ["e2_i_", "epaac", "einschaetzung", "assessment"],
}


ALIASES = {
    "tbImportLabsData": {
        "caseid": "coCaseId",
        "case_id": "coCaseId",
        "cas": "coCaseId",
        "specdt": "coSpecimen_datetime",
        "specimen_datetime": "coSpecimen_datetime",
        "na": "coSodium_mmol_L",
        "na_flag": "coSodium_flag",
        "na_low": "cosodium_ref_low",
        "na_high": "cosodium_ref_high",
        "k": "coPotassium_mmol_L",
        "k_flag": "coPotassium_flag",
        "k_low": "coPotassium_ref_low",
        "k_high": "coPotassium_ref_high",
        "creat": "coCreatinine_mg_dL",
        "creat_flag": "coCreatinine_flag",
        "creat_low": "coCreatinine_ref_low",
        "creat_high": "coCreatinine_ref_high",
        "egfr": "coEgfr_mL_min_1_73m2",
        "egfr_flag": "coEgfr_flag",
        "egfr_low": "coEgfr_ref_low",
        "egfr_high": "coEgfr_ref_high",
        "gluc": "coGlucose_mg_dL",
        "gluc_flag": "coGlucose_flag",
        "gluc_low": "coGlucose_ref_low",
        "gluc_high": "coGlucose_ref_high",
        "hb": "coHemoglobin_g_dL",
        "hb_flag": "coHb_flag",
        "hb_low": "coHb_ref_low",
        "hb_high": "coHb_ref_high",
        "wbc": "coWbc_10e9_L",
        "wbc_flag": "coWbc_flag",
        "wbc_low": "coWbc_ref_low",
        "wbc_high": "coWbc_ref_high",
        "plt": "coPlatelets_10e9_L",
        "plt_flag": "coPlatelets_flag",
        "plt_low": "coPlt_ref_low",
        "plt_high": "coPlt_ref_high",
        "crp": "coCrp_mg_L",
        "crp_flag": "coCrp_flag",
        "crp_low": "coCrp_ref_low",
        "crp_high": "coCrp_ref_high",
        "alt": "coAlt_U_L",
        "alt_flag": "coAlt_flag",
        "alt_low": "coAlt_ref_low",
        "alt_high": "coAlt_ref_high",
        "ast": "coAst_U_L",
        "ast_flag": "coAst_flag",
        "ast_low": "coAst_ref_low",
        "ast_high": "coAst_ref_high",
        "bili": "coBilirubin_mg_dL",
        "bili_flag": "coBilirubin_flag",
        "bili_low": "coBili_ref_low",
        "bili_high": "coBili_ref_high",
        "alb": "coAlbumin_g_dL",
        "alb_flag": "coAlbumin_flag",
        "alb_low": "coAlbumin_ref_low",
        "alb_high": "coAlbumin_ref_high",
        "inr": "coInr",
        "inr_flag": "coInr_flag",
        "inr_low": "coInr_ref_low",
        "inr_high": "coInr_ref_high",
        "lact": "coLactate_mmol_L",
        "lact_flag": "coLactate_flag",
        "lact_low": "coLactate_ref_low",
        "lact_high": "coLactate_ref_high",
    },
    "tbImportIcd10Data": {
        "caseid": "coCaseId",
        "case_id": "coCaseId",
        "station": "coWard",
        "ward": "coWard",
        "aufnahmedatum": "coAdmission_date",
        "admission_date": "coAdmission_date",
        "entlassungsdatum": "coDischarge_date",
        "discharge_date": "coDischarge_date",
        "verweildauer_tage": "coLength_of_stay_days",
        "length_of_stay_days": "coLength_of_stay_days",
        "icd10_haupt": "coPrimary_icd10_code",
        "primary_icd10_code": "coPrimary_icd10_code",
        "icd10_haupt_bezeichnung": "coPrimary_icd10_description_en",
        "primary_icd10_description_en": "coPrimary_icd10_description_en",
        "icd10_neben": "coSecondary_icd10_codes",
        "secondary_icd10_codes": "coSecondary_icd10_codes",
        "icd10_neben_bezeichnung": "cpSecondary_icd10_descriptions_en",
        "secondary_icd10_descriptions_en": "cpSecondary_icd10_descriptions_en",
        "ops_code": "coOps_codes",
        "ops_codes": "coOps_codes",
        "ops_bezeichnung": "ops_descriptions_en",
        "ops_descriptions_en": "ops_descriptions_en",
        "id_cas": "coCaseId",
        "date_ad": "coAdmission_date",
        "d_s": "coPrimary_icd10_code",
        "d_s_str": "coPrimary_icd10_description_en",
        "d_m": "coSecondary_icd10_codes",
        "d_m_str": "cpSecondary_icd10_descriptions_en",
        "proc": "coOps_codes",
        "proc_str": "ops_descriptions_en",
    },
    "tbImportDeviceMotionData": {
        "timestamp": "coTimestamp",
        "date": "coTimestamp",
        "patient_id": "coPatient_id",
        "id": "coPatient_id",
        "movement_index_0_100": "coMovement_index_0_100",
        "idx_mov": "coMovement_index_0_100",
        "micro_movements_count": "coMicro_movements_count",
        "num_mov": "coMicro_movements_count",
        "bed_exit_detected_0_1": "coBed_exit_detected_0_1",
        "num_ex": "coBed_exit_detected_0_1",
        "fall_event_0_1": "coFall_event_0_1",
        "fall": "coFall_event_0_1",
        "impact_magnitude_g": "coImpact_magnitude_g",
        "mag_impact": "coImpact_magnitude_g",
        "post_fall_immobility_minutes": "coPost_fall_immobility_minutes",
        "min_immob": "coPost_fall_immobility_minutes",
    },
    "tbImportDevice1HzMotionData": {
        "timestamp": "coTimestamp",
        "date": "coTimestamp",
        "patient_id": "coPatient_id",
        "id_pat": "coPatient_id",
        "patientid": "coPatient_id",
        "device_id": "coDevice_id",
        "id_dev": "coDevice_id",
        "deviceid": "coDevice_id",
        "bed_occupied_0_1": "coBed_occupied_0_1",
        "bedoccupied": "coBed_occupied_0_1",
        "movement_score_0_100": "coMovement_score_0_100",
        "movementscore": "coMovement_score_0_100",
        "accel_x_m_s2": "coAccel_x_m_s2",
        "accelx": "coAccel_x_m_s2",
        "accx": "coAccel_x_m_s2",
        "accel_y_m_s2": "coAccel_y_m_s2",
        "accely": "coAccel_y_m_s2",
        "accy": "coAccel_y_m_s2",
        "accel_z_m_s2": "coAccel_z_m_s2",
        "accelz": "coAccel_z_m_s2",
        "accz": "coAccel_z_m_s2",
        "accel_magnitude_g": "coAccel_magnitude_g",
        "accelmag": "coAccel_magnitude_g",
        "acc_mag": "coAccel_magnitude_g",
        "pressure_zone1_0_100": "coPressure_zone1_0_100",
        "pressz1": "coPressure_zone1_0_100",
        "p1": "coPressure_zone1_0_100",
        "pressure_zone2_0_100": "coPressure_zone2_0_100",
        "pressz2": "coPressure_zone2_0_100",
        "p2": "coPressure_zone2_0_100",
        "pressure_zone3_0_100": "coPressure_zone3_0_100",
        "pressz3": "coPressure_zone3_0_100",
        "p3": "coPressure_zone3_0_100",
        "pressure_zone4_0_100": "coPressure_zone4_0_100",
        "pressz4": "coPressure_zone4_0_100",
        "p4": "coPressure_zone4_0_100",
        "bed_exit_event_0_1": "coBed_exit_event_0_1",
        "bedexit": "coBed_exit_event_0_1",
        "exit": "coBed_exit_event_0_1",
        "bed_return_event_0_1": "coBed_return_event_0_1",
        "bedreturn": "coBed_return_event_0_1",
        "return": "coBed_return_event_0_1",
        "fall_event_0_1": "coFall_event_0_1",
        "fallevent": "coFall_event_0_1",
        "fall": "coFall_event_0_1",
        "impact_magnitude_g": "coImpact_magnitude_g",
        "impactmag": "coImpact_magnitude_g",
        "event_id": "coEvent_id",
        "eventid": "coEvent_id",
    },
    "tbImportMedicationInpatientData": {
        "record_type": "coRecord_type",
        "rec_type": "coRecord_type",
        "patient_id": "coPatient_id",
        "pat_id": "coPatient_id",
        "encounter_id": "coEncounter_id",
        "enc_id": "coEncounter_id",
        "ward": "coWard",
        "station": "coWard",
        "admission_datetime": "coAdmission_datetime",
        "aufnahme_dt": "coAdmission_datetime",
        "discharge_datetime": "coDischarge_datetime",
        "entlassung_dt": "coDischarge_datetime",
        "order_id": "coOrder_id",
        "order_uuid": "coOrder_uuid",
        "uuid": "coOrder_uuid",
        "medication_code_atc": "coMedication_code_atc",
        "atc_code": "coMedication_code_atc",
        "medication_name": "coMedication_name",
        "medikament": "coMedication_name",
        "route": "coRoute",
        "applikation": "coRoute",
        "dose": "coDose",
        "dosis": "coDose",
        "dose_unit": "coDose_unit",
        "einheit": "coDose_unit",
        "frequency": "coFrequency",
        "haeufigkeit": "coFrequency",
        "order_start_datetime": "coOrder_start_datetime",
        "start_dt": "coOrder_start_datetime",
        "order_stop_datetime": "coOrder_stop_datetime",
        "stop_dt": "coOrder_stop_datetime",
        "is_prn_0_1": "coIs_prn_0_1",
        "bei_bedarf": "coIs_prn_0_1",
        "indication": "coIndication",
        "indikation": "coIndication",
        "prescriber_role": "prescriber_role",
        "verschreiber": "prescriber_role",
        "order_status": "order_status",
        "bestellung_status": "order_status",
        "administration_datetime": "administration_datetime",
        "gabe_dt": "administration_datetime",
        "administered_dose": "administered_dose",
        "gegebene_dosis": "administered_dose",
        "administered_unit": "administered_unit",
        "gabe_einheit": "administered_unit",
        "administration_status": "administration_status",
        "gabe_status": "administration_status",
        "note": "note",
        "notiz": "note",
    },
    "tbImportNursingDailyReportsData": {
        "caseid": "coCaseId",
        "case_id": "coCaseId",
        "cas": "coCaseId",
        "patient_id": "coPatient_id",
        "patientid": "coPatient_id",
        "pat": "coPatient_id",
        "ward": "coWard",
        "war": "coWard",
        "report_date": "coReport_date",
        "reportdate": "coReport_date",
        "dat": "coReport_date",
        "shift": "coShift",
        "shf": "coShift",
        "nursing_note_free_text": "coNursing_note_free_text",
        "nursingnote": "coNursing_note_free_text",
        "txt": "coNursing_note_free_text",
    },
}


def normalize(name: str) -> str:
    return re.sub(r"[^a-z0-9_]", "", name.strip().lower())


def dedupe_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for i, raw in enumerate(headers, start=1):
        base = (raw or "").strip() or f"col_{i}"
        count = seen.get(base, 0) + 1
        seen[base] = count
        out.append(base if count == 1 else f"{base}__{count}")
    return out


def parse_schema(sql_path: Path) -> dict[str, list[str]]:
    schema: dict[str, list[str]] = {}
    current_table: str | None = None
    for raw_line in sql_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("/*"):
            continue
        create_match = CREATE_RE.match(line)
        if create_match:
            current_table = create_match.group(1)
            schema[current_table] = []
            continue
        if current_table is None:
            continue
        if line.startswith(")"):
            current_table = None
            continue
        if line.lower().startswith("constraint"):
            continue
        parts = line.rstrip(",").split()
        if len(parts) >= 2:
            schema[current_table].append(parts[0])
    return schema


def sniff_text_file(path: Path) -> tuple[str, str]:
    for enc in ("utf-8-sig", "cp1252", "latin1"):
        try:
            text = path.read_text(encoding=enc)
            first = text.splitlines()[0] if text.splitlines() else ""
            delim = ";" if first.count(";") > first.count(",") else ","
            return enc, delim
        except UnicodeDecodeError:
            continue
    return "latin1", ","


def cell_to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    return str(value)


def strip_trailing_empty(row: list[str]) -> list[str]:
    out = row[:]
    while out and out[-1] == "":
        out.pop()
    return out


def clean_sql_identifier(name: str) -> str:
    token = name.strip()
    if not token:
        return token
    if token.startswith("[") and token.endswith("]"):
        token = token[1:-1]
    elif token.startswith("`") and token.endswith("`"):
        token = token[1:-1]
    elif token.startswith('"') and token.endswith('"'):
        token = token[1:-1]
    return token.strip()


def clean_sql_table_name(name: str) -> str:
    parts = [clean_sql_identifier(p) for p in name.split(".") if p.strip()]
    if not parts:
        return clean_sql_identifier(name)
    return parts[-1]


def split_sql_csv(segment: str) -> list[str]:
    values: list[str] = []
    buf: list[str] = []
    in_single = False
    in_double = False
    depth = 0
    i = 0
    while i < len(segment):
        ch = segment[i]
        nxt = segment[i + 1] if i + 1 < len(segment) else ""
        if ch == "'" and not in_double:
            if in_single and nxt == "'":
                buf.append("''")
                i += 2
                continue
            in_single = not in_single
            buf.append(ch)
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            buf.append(ch)
            i += 1
            continue
        if not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")" and depth > 0:
                depth -= 1
            elif ch == "," and depth == 0:
                values.append("".join(buf).strip())
                buf = []
                i += 1
                continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        values.append(tail)
    return values


def extract_sql_value_tuples(values_blob: str) -> list[str]:
    tuples: list[str] = []
    depth = 0
    start = -1
    in_single = False
    in_double = False
    i = 0
    while i < len(values_blob):
        ch = values_blob[i]
        nxt = values_blob[i + 1] if i + 1 < len(values_blob) else ""
        if ch == "'" and not in_double:
            if in_single and nxt == "'":
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            i += 1
            continue
        if not in_single and not in_double:
            if ch == "(":
                if depth == 0:
                    start = i + 1
                depth += 1
            elif ch == ")" and depth > 0:
                depth -= 1
                if depth == 0 and start >= 0:
                    tuples.append(values_blob[start:i])
                    start = -1
        i += 1
    return tuples


def parse_sql_literal(raw: str) -> str:
    token = raw.strip()
    if not token:
        return ""
    up = token.upper()
    if up == "NULL" or up == "DEFAULT":
        return ""
    if token[:2].lower() == "n'" and token.endswith("'"):
        token = token[1:]
    if token.startswith("'") and token.endswith("'") and len(token) >= 2:
        return token[1:-1].replace("''", "'").strip()
    if token.startswith('"') and token.endswith('"') and len(token) >= 2:
        return token[1:-1].replace('""', '"').strip()
    return token.strip()


def parse_sql_insert_statements(text: str) -> list[dict[str, Any]]:
    statements: list[dict[str, Any]] = []
    for match in SQL_INSERT_RE.finditer(text):
        raw_table = match.group(1) or ""
        raw_columns = match.group(2) or ""
        raw_values = match.group(3) or ""
        table_name = clean_sql_table_name(raw_table)
        columns = [clean_sql_identifier(c) for c in split_sql_csv(raw_columns)] if raw_columns else []
        tuple_blobs = extract_sql_value_tuples(raw_values)
        rows: list[list[str]] = []
        for blob in tuple_blobs:
            values = [parse_sql_literal(v) for v in split_sql_csv(blob)]
            rows.append(values)
        statements.append(
            {
                "table_name": table_name,
                "columns": columns,
                "rows": rows,
            }
        )
    return statements


def rows_to_records(
    rows: list[list[str]], table: str, profile: dict[str, Any]
) -> tuple[list[str], list[dict[str, Any]], dict[str, Any]]:
    if not rows:
        return [], [], profile

    row_width = max(len(r) for r in rows) if rows else 0
    normalized_rows = [r + [""] * (row_width - len(r)) for r in rows]

    if table == "tbImportAcData" and len(normalized_rows) >= 3:
        row2 = normalized_rows[1]
        if sum(1 for c in row2 if "E" in c and "_I_" in c) >= 10:
            headers = row2
            data_rows = normalized_rows[2:]
            profile["ac_mode"] = "row2_iids_header"
        else:
            headers = normalized_rows[0]
            data_rows = normalized_rows[1:]
            profile["ac_mode"] = "row1_header"
    else:
        headers = normalized_rows[0]
        data_rows = normalized_rows[1:]

    header_like = sum(1 for h in headers if re.search(r"[A-Za-z_]", h or "")) >= 1
    if not header_like:
        headers = [f"col_{i+1}" for i in range(len(normalized_rows[0]))]
        data_rows = normalized_rows
        profile["header_generated"] = True

    headers = dedupe_headers(headers)
    records = [{headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))} for row in data_rows]
    return headers, records, profile


def load_csv_records(path: Path, table: str) -> tuple[list[str], list[dict[str, Any]], dict[str, Any]]:
    enc, delim = sniff_text_file(path)
    with path.open(encoding=enc, newline="") as f:
        rows = list(csv.reader(f, delimiter=delim))
    profile: dict[str, Any] = {"encoding": enc, "delimiter": delim, "format": "csv"}
    rows = [strip_trailing_empty([cell_to_str(c).strip() for c in row]) for row in rows]
    rows = [r for r in rows if any(c != "" for c in r)]
    return rows_to_records(rows, table, profile)


def load_xlsx_records(path: Path, table: str) -> tuple[list[str], list[dict[str, Any]], dict[str, Any]]:
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]
    raw_rows = []
    for row in ws.iter_rows(values_only=True):
        normalized = strip_trailing_empty([cell_to_str(c).strip() for c in row])
        if any(c != "" for c in normalized):
            raw_rows.append(normalized)
    wb.close()

    profile: dict[str, Any] = {"format": "xlsx", "sheet": ws.title}
    if not raw_rows:
        return [], [], profile

    first = raw_rows[0]
    first_cell = first[0] if first else ""
    if first_cell and ("," in first_cell or ";" in first_cell):
        other_cells_empty = all(c == "" for c in first[1:])
        if other_cells_empty:
            delim = ";" if first_cell.count(";") > first_cell.count(",") else ","
            text_lines: list[str] = []
            for row in raw_rows:
                pieces = [c for c in row if c != ""]
                if not pieces:
                    continue
                text_lines.append(delim.join(pieces))
            csv_rows = [strip_trailing_empty(r) for r in csv.reader(text_lines, delimiter=delim)]
            csv_rows = [r for r in csv_rows if any(c != "" for c in r)]
            profile["xlsx_mode"] = "csv_in_cells"
            profile["delimiter"] = delim
            return rows_to_records(csv_rows, table, profile)

    profile["xlsx_mode"] = "tabular"
    return rows_to_records(raw_rows, table, profile)


def extract_nursing_page_record(page_text: str) -> dict[str, str]:
    flat = " ".join((page_text or "").split())
    m_date = PDF_DATE_RE.search(flat)
    m_shift = PDF_SHIFT_RE.search(flat)
    m_pat = PDF_PAT_RE.search(flat)
    m_case = PDF_CASE_RE.search(flat)
    m_ward = PDF_WARD_RE.search(flat)
    report_text = ""
    report_marker = re.search(r"\bReport\b", flat, re.IGNORECASE)
    if report_marker:
        report_text = flat[report_marker.end() :].strip()
    return {
        "case_id": m_case.group(1).strip() if m_case else "",
        "patient_id": m_pat.group(1).strip() if m_pat else "",
        "ward": m_ward.group(1).strip() if m_ward else "",
        "report_date": m_date.group(1).strip() if m_date else "",
        "shift": m_shift.group(1).strip() if m_shift else "",
        "nursing_note_free_text": report_text,
    }


def load_pdf_records(path: Path, table: str) -> tuple[list[str], list[dict[str, Any]], dict[str, Any]]:
    profile: dict[str, Any] = {"format": "pdf"}
    with pdfplumber.open(path) as pdf:
        pages = pdf.pages
        profile["pages"] = len(pages)

        if table == "tbImportNursingDailyReportsData":
            rows: list[list[str]] = []
            for page in pages:
                record = extract_nursing_page_record(page.extract_text() or "")
                rows.append(
                    [
                        record["case_id"],
                        record["patient_id"],
                        record["ward"],
                        record["report_date"],
                        record["shift"],
                        record["nursing_note_free_text"],
                    ]
                )
            profile["pdf_mode"] = "page_text_nursing"
            return rows_to_records(
                [["case_id", "patient_id", "ward", "report_date", "shift", "nursing_note_free_text"]]
                + rows,
                table,
                profile,
            )

        table_rows: list[list[str]] = []
        for page in pages:
            for table_data in page.extract_tables() or []:
                for row in table_data:
                    if not row:
                        continue
                    normalized = strip_trailing_empty([cell_to_str(c).strip() for c in row])
                    if any(c != "" for c in normalized):
                        table_rows.append(normalized)
        if table_rows:
            profile["pdf_mode"] = "table_extract"
            return rows_to_records(table_rows, table, profile)

        doc_lines: list[str] = []
        for page in pages:
            text = page.extract_text() or ""
            for line in text.splitlines():
                clean = line.strip()
                if clean:
                    doc_lines.append(clean)
        profile["pdf_mode"] = "document_text_fallback"
        if not doc_lines:
            return [], [], profile
        return rows_to_records([["text"], ["\n".join(doc_lines)]], table, profile)


def load_sql_records(path: Path, table: str) -> tuple[list[str], list[dict[str, Any]], dict[str, Any]]:
    enc, _ = sniff_text_file(path)
    text = path.read_text(encoding=enc)
    statements = parse_sql_insert_statements(text)
    profile: dict[str, Any] = {
        "format": "sql",
        "encoding": enc,
        "insert_statements": len(statements),
    }
    if not statements:
        return [], [], profile

    grouped: dict[str, list[dict[str, Any]]] = {}
    for stmt in statements:
        name = stmt["table_name"] or "unknown"
        grouped.setdefault(name, []).append(stmt)

    best_table = max(grouped.keys(), key=lambda t: sum(len(s["rows"]) for s in grouped[t]))
    selected = grouped[best_table]
    profile["sql_table"] = best_table
    profile["sql_tables_found"] = sorted(grouped.keys())

    headers: list[str] = []
    max_len_without_cols = 0
    for stmt in selected:
        cols = stmt["columns"]
        if cols:
            for col in cols:
                if col and col not in headers:
                    headers.append(col)
        else:
            for row in stmt["rows"]:
                if len(row) > max_len_without_cols:
                    max_len_without_cols = len(row)

    if not headers:
        headers = [f"col_{i+1}" for i in range(max_len_without_cols)]

    data_rows: list[list[str]] = []
    for stmt in selected:
        cols = stmt["columns"]
        for row in stmt["rows"]:
            out = [""] * len(headers)
            if cols:
                for idx, col in enumerate(cols):
                    if idx >= len(row):
                        continue
                    if col not in headers:
                        continue
                    out[headers.index(col)] = row[idx]
            else:
                for idx, val in enumerate(row[: len(headers)]):
                    out[idx] = val
            data_rows.append(out)

    return rows_to_records([headers] + data_rows, table, profile)


def load_records(path: Path, table: str) -> tuple[list[str], list[dict[str, Any]], dict[str, Any]]:
    if path.suffix.lower() == ".xlsx":
        return load_xlsx_records(path, table)
    if path.suffix.lower() == ".pdf":
        return load_pdf_records(path, table)
    if path.suffix.lower() == ".sql":
        return load_sql_records(path, table)
    return load_csv_records(path, table)


def detect_table(
    path: Path, headers: list[str], profile: dict[str, Any] | None = None
) -> str | None:
    name = path.name.lower()
    if profile and profile.get("sql_table"):
        sql_name = normalize(str(profile["sql_table"]))
        target_by_norm = {normalize(t): t for t in TABLE_HINTS}
        if sql_name in target_by_norm:
            return target_by_norm[sql_name]
        if "1hz" in sql_name:
            return "tbImportDevice1HzMotionData"
        if "device" in sql_name:
            return "tbImportDeviceMotionData"
        if "labs" in sql_name or "lab" in sql_name:
            return "tbImportLabsData"
        if "icd" in sql_name or "ops" in sql_name:
            return "tbImportIcd10Data"
        if "med" in sql_name:
            return "tbImportMedicationInpatientData"
        if "nursing" in sql_name or "report" in sql_name:
            return "tbImportNursingDailyReportsData"
        if "acdata" in sql_name or "epaac" in sql_name:
            return "tbImportAcData"

    if "1hz" in name:
        return "tbImportDevice1HzMotionData"
    if "device" in name:
        return "tbImportDeviceMotionData"
    if "labs" in name:
        return "tbImportLabsData"
    if "icd" in name or "ops" in name:
        return "tbImportIcd10Data"
    if "med" in name:
        return "tbImportMedicationInpatientData"
    if "nursing" in name or "report" in name:
        return "tbImportNursingDailyReportsData"
    if "epaac" in name:
        return "tbImportAcData"

    joined = " ".join(headers).lower()
    for table, hints in TABLE_HINTS.items():
        if all(h in joined for h in hints[:1]):
            return table
    return None


def detect_table_from_records(records: list[dict[str, Any]]) -> str | None:
    if not records:
        return None
    blob_parts: list[str] = []
    for rec in records[:5]:
        for _, val in rec.items():
            s = str(val or "").strip().lower()
            if s:
                blob_parts.append(s)
    blob = " ".join(blob_parts)
    if not blob:
        return None
    scores: dict[str, int] = {}
    for table, hints in CONTENT_HINTS.items():
        scores[table] = sum(1 for h in hints if h in blob)
    best_table = max(scores, key=scores.get)
    return best_table if scores.get(best_table, 0) >= 2 else None


DEFAULT_PROMPT_TEMPLATE = (
    "Map source columns to target SQL columns.\n"
    "Return JSON only.\n"
    "Keys: source columns exactly as given.\n"
    "Values: one target column from target list or null.\n"
    "Never invent target columns.\n\n"
    "source_columns={{SOURCE_COLUMNS}}\n"
    "target_columns={{TARGET_COLUMNS}}\n"
)

DEFAULT_TABLE_DETECT_PROMPT = (
    "You are routing one healthcare source file to one SQL target table.\n"
    "Return JSON only in this shape: {\"table\": \"<table_name_or_null>\"}.\n"
    "Choose only from allowed_tables or null.\n\n"
    "file_name={{FILE_NAME}}\n"
    "source_headers={{SOURCE_HEADERS}}\n"
    "sample_rows={{SAMPLE_ROWS}}\n"
    "allowed_tables={{ALLOWED_TABLES}}\n"
)

DEFAULT_EXTRACTION_PROMPT = (
    "Extract structured SQL column values from clinical free text.\n"
    "Return JSON object only.\n"
    "Keys must be from target_columns.\n"
    "Values must be string or null.\n"
    "Do not invent facts; if missing, use null.\n\n"
    "table={{TABLE}}\n"
    "target_columns={{TARGET_COLUMNS}}\n"
    "source_text={{SOURCE_TEXT}}\n"
)


def build_prompt(
    source_columns: list[str], free_targets: list[str], prompt_template: str | None
) -> str:
    template = prompt_template or DEFAULT_PROMPT_TEMPLATE
    return (
        template.replace("{{SOURCE_COLUMNS}}", json.dumps(source_columns)).replace(
            "{{TARGET_COLUMNS}}", json.dumps(free_targets)
        )
    )


def run_llama_mapping(
    model: str,
    source_columns: list[str],
    free_targets: list[str],
    timeout_s: int,
    prompt_template: str | None,
) -> dict[str, str | None]:
    if not source_columns or not free_targets:
        return {}
    prompt = build_prompt(
        source_columns=source_columns,
        free_targets=free_targets,
        prompt_template=prompt_template,
    )
    try:
        result = subprocess.run(
            ["ollama", "run", model, prompt],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}

    text = (result.stdout or "").strip()
    match = JSON_BLOCK_RE.search(text)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
        out: dict[str, str | None] = {}
        for key, value in parsed.items():
            out[str(key)] = None if value is None else str(value)
        return out
    except json.JSONDecodeError:
        return {}


def run_llama_table_detection(
    model: str,
    path: Path,
    headers: list[str],
    records: list[dict[str, Any]],
    allowed_tables: list[str],
    timeout_s: int,
) -> str | None:
    if not allowed_tables:
        return None
    sample_rows = records[:3]
    prompt = (
        DEFAULT_TABLE_DETECT_PROMPT.replace("{{FILE_NAME}}", json.dumps(path.name))
        .replace("{{SOURCE_HEADERS}}", json.dumps(headers[:80], ensure_ascii=False))
        .replace("{{SAMPLE_ROWS}}", json.dumps(sample_rows, ensure_ascii=False))
        .replace("{{ALLOWED_TABLES}}", json.dumps(sorted(allowed_tables)))
    )
    try:
        result = subprocess.run(
            ["ollama", "run", model, prompt],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    text = (result.stdout or "").strip()
    match = JSON_BLOCK_RE.search(text)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    table = parsed.get("table") if isinstance(parsed, dict) else None
    if table is None:
        return None
    table_s = str(table).strip()
    return table_s if table_s in allowed_tables else None


def build_row_extraction_prompt(
    table: str, target_cols: list[str], source_text: str
) -> str:
    return (
        DEFAULT_EXTRACTION_PROMPT.replace("{{TABLE}}", json.dumps(table))
        .replace("{{TARGET_COLUMNS}}", json.dumps(target_cols))
        .replace("{{SOURCE_TEXT}}", json.dumps(source_text[:6000], ensure_ascii=False))
    )


def run_llama_row_extraction(
    model: str,
    table: str,
    target_cols: list[str],
    source_text: str,
    timeout_s: int,
) -> dict[str, str]:
    if not source_text.strip():
        return {}
    prompt = build_row_extraction_prompt(table, target_cols, source_text)
    try:
        result = subprocess.run(
            ["ollama", "run", model, prompt],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}
    text = (result.stdout or "").strip()
    match = JSON_BLOCK_RE.search(text)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    allowed = set(target_cols)
    out: dict[str, str] = {}
    for key, value in parsed.items():
        col = str(key)
        if col not in allowed or value is None:
            continue
        val = str(value).strip()
        if val:
            out[col] = val
    return out


def rec_to_text(rec: dict[str, Any], preferred_cols: list[str]) -> str:
    parts: list[str] = []
    if preferred_cols:
        for col in preferred_cols:
            val = str(rec.get(col, "") or "").strip()
            if val:
                parts.append(f"{col}: {val}")
    else:
        for col, raw_val in rec.items():
            val = str(raw_val or "").strip()
            if val:
                parts.append(f"{col}: {val}")
    return "\n".join(parts)


def detect_text_columns(headers: list[str], records: list[dict[str, Any]]) -> list[str]:
    text_cols = [h for h in headers if TEXTY_COL_RE.search(h or "")]
    if text_cols:
        return text_cols
    if not records:
        return []
    scored: list[tuple[float, str]] = []
    for h in headers:
        lengths = [len(str(r.get(h, "") or "").strip()) for r in records[:30]]
        non_empty = [n for n in lengths if n > 0]
        if not non_empty:
            continue
        avg_len = sum(non_empty) / len(non_empty)
        if avg_len >= 40:
            scored.append((avg_len, h))
    scored.sort(reverse=True)
    return [h for _, h in scored[:2]]


def should_use_semantic_extraction(
    headers: list[str],
    records: list[dict[str, Any]],
    mapping: dict[str, str],
) -> bool:
    if not records:
        return False
    if len(records) > 200:
        return False
    text_cols = detect_text_columns(headers, records)
    if text_cols:
        return True
    if not headers:
        return False
    mapped_ratio = len(mapping) / max(len(headers), 1)
    return mapped_ratio < 0.2


def enrich_rows_with_semantic_extraction(
    table: str,
    headers: list[str],
    records: list[dict[str, Any]],
    target_cols: list[str],
    rows: list[dict[str, Any]],
    model: str,
    timeout_s: int,
) -> tuple[list[dict[str, Any]], int]:
    text_cols = detect_text_columns(headers, records)
    enriched = 0
    for idx, rec in enumerate(records):
        source_text = rec_to_text(rec, text_cols)
        extracted = run_llama_row_extraction(
            model=model,
            table=table,
            target_cols=target_cols,
            source_text=source_text,
            timeout_s=timeout_s,
        )
        if not extracted:
            continue
        row = rows[idx] if idx < len(rows) else {c: "" for c in target_cols}
        changed = False
        for col, val in extracted.items():
            if not row.get(col):
                row[col] = val
                changed = True
        if idx >= len(rows):
            rows.append(row)
        if changed:
            enriched += 1
    return rows, enriched


def build_mapping(
    table: str,
    headers: list[str],
    target_cols: list[str],
    use_llm: bool,
    model: str,
    timeout_s: int,
    prompt_template: str | None,
) -> dict[str, str]:
    target_set = set(target_cols)
    mapping: dict[str, str] = {}
    alias_table = ALIASES.get(table, {})

    # 1) Exact normalized match: source -> co<source>
    target_normalized = {normalize(c.removeprefix("co")): c for c in target_cols}
    for source_col in headers:
        src_norm = normalize(source_col)
        if src_norm in alias_table:
            dest = alias_table[src_norm]
            if dest in target_set:
                mapping[source_col] = dest
                continue
        if table == "tbImportAcData":
            codes = EPA_CODE_RE.findall(source_col)
            for code in codes:
                candidate = "co" + code.replace("_", "").upper()
                if candidate in target_set:
                    mapping[source_col] = candidate
                    break
            if source_col in mapping:
                continue
        dest = target_normalized.get(src_norm)
        if dest:
            mapping[source_col] = dest

    # 2) Llama fallback for unresolved headers
    unresolved = [h for h in headers if h not in mapping]
    free_targets = [c for c in target_cols if c not in mapping.values()]
    if use_llm and unresolved and free_targets:
        suggestions = run_llama_mapping(
            model, unresolved, free_targets, timeout_s, prompt_template
        )
        for src, dest in suggestions.items():
            if src in unresolved and dest in target_set:
                mapping[src] = dest

    return mapping


def standardize_records(
    records: list[dict[str, Any]], target_cols: list[str], mapping: dict[str, str]
) -> list[dict[str, Any]]:
    out_rows: list[dict[str, Any]] = []
    for rec in records:
        out = {c: "" for c in target_cols}
        for src, dest in mapping.items():
            if src not in rec:
                continue
            val = rec[src]
            if val is None:
                continue
            val = str(val).strip()
            if not val:
                continue
            out[dest] = val
        out_rows.append(out)
    return out_rows


def write_csv(path: Path, rows: list[dict[str, Any]], cols: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-sql", required=True, type=Path)
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--model", default="llama3.2:latest")
    parser.add_argument("--llm-timeout", default=45, type=int)
    parser.add_argument("--prompt-template", type=Path)
    parser.add_argument("--no-llm", action="store_true")
    args = parser.parse_args()

    schema = parse_schema(args.schema_sql)
    in_dir = args.input_dir
    out_dir = args.output_dir
    report: list[dict[str, Any]] = []
    prompt_template = None
    if args.prompt_template:
        prompt_template = args.prompt_template.read_text(encoding="utf-8")

    files = sorted(
        [
            p
            for p in in_dir.rglob("*")
            if p.suffix.lower() in {".csv", ".xlsx", ".pdf", ".sql"}
        ]
    )
    for path in files:
        try:
            headers_probe, records_probe, profile_probe = load_records(path, "tbImportAcData")
            table = detect_table(path, headers_probe, profile_probe)
            if not table:
                table = detect_table_from_records(records_probe)
            if not table and not args.no_llm:
                allowed_tables = [t for t in schema.keys() if t in TABLE_HINTS]
                table = run_llama_table_detection(
                    model=args.model,
                    path=path,
                    headers=headers_probe,
                    records=records_probe,
                    allowed_tables=allowed_tables or list(schema.keys()),
                    timeout_s=args.llm_timeout,
                )
            if not table or table not in schema:
                report.append(
                    {
                        "file": str(path),
                        "status": "skipped",
                        "reason": "table_not_detected_or_not_in_schema",
                    }
                )
                continue

            headers, records, profile = load_records(path, table)
            target_cols = [c for c in schema[table] if c != "coId"]
            mapping = build_mapping(
                table=table,
                headers=headers,
                target_cols=target_cols,
                use_llm=not args.no_llm,
                model=args.model,
                timeout_s=args.llm_timeout,
                prompt_template=prompt_template,
            )
            rows = standardize_records(records, target_cols, mapping)
            semantic_rows_enriched = 0
            semantic_used = False
            if not args.no_llm and should_use_semantic_extraction(headers, records, mapping):
                rows, semantic_rows_enriched = enrich_rows_with_semantic_extraction(
                    table=table,
                    headers=headers,
                    records=records,
                    target_cols=target_cols,
                    rows=rows,
                    model=args.model,
                    timeout_s=args.llm_timeout,
                )
                semantic_used = True

            ext = path.suffix.lower().lstrip(".")
            output_name = f"{path.stem}__{ext}__{table}.csv"
            output_path = out_dir / output_name
            write_csv(output_path, rows, target_cols)

            report.append(
                {
                    "file": str(path),
                    "table": table,
                    "status": "ok",
                    "rows_in": len(records),
                    "rows_out": len(rows),
                    "headers_in": len(headers),
                    "targets_total": len(target_cols),
                    "mapped_columns": len(mapping),
                    "unmapped_headers": [h for h in headers if h not in mapping],
                    "semantic_extraction_used": semantic_used,
                    "semantic_rows_enriched": semantic_rows_enriched,
                    "profile": profile,
                }
            )
        except Exception as exc:
            report.append(
                {
                    "file": str(path),
                    "status": "error",
                    "reason": str(exc),
                }
            )
            continue

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "standardization_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
