#!/usr/bin/env python3
"""Build a manually curated 15-case gold manifest and expected artifacts."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import standardize as std


def to_rel(path: Path, repo_root: Path) -> str:
    return str(path.resolve().relative_to(repo_root.resolve()))


def write_expected_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)


def build_rows(
    records: list[dict[str, Any]],
    mapping: dict[str, str | None],
    output_columns: list[str],
    max_rows: int,
) -> list[dict[str, str]]:
    positive = {k: v for k, v in mapping.items() if v}
    out: list[dict[str, str]] = []
    for rec in records[:max_rows]:
        row = {c: "" for c in output_columns}
        for src, dest in positive.items():
            if dest in row:
                row[dest] = str(rec.get(src, "") or "").strip()
        out.append(row)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=Path.cwd(), type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--max-rows", default=3, type=int)
    args = parser.parse_args()

    root = args.repo_root.resolve()
    out_dir = args.output_dir.resolve()
    exp_map_dir = out_dir / "expected_mappings"
    exp_out_dir = out_dir / "expected_outputs"
    exp_map_dir.mkdir(parents=True, exist_ok=True)
    exp_out_dir.mkdir(parents=True, exist_ok=True)

    # Curated cases (mixed CSV/XLSX/PDF, hard variants, explicit mappings).
    cases = [
        {
            "id": "manual_001_ac_csv",
            "input_file": "Endtestdaten_mit_Fehlern_ einheitliche ID/epaAC-Data-3.csv",
            "expected_table": "tbImportAcData",
            "mapping": {
                "E2_I_226": "coE2I226",
                "E2_I_222 (E3_I_0889)(STRING)": "coE2I222",
                "E2_I_225": "coE2I225",
                "E0_I_001": "coE0I001",
                "E0_I_002": "coE0I002",
                "E0_I_003": "coE0I003",
            },
            "output_columns": ["coE2I226", "coE2I222", "coE2I225", "coE0I001", "coE0I002"],
        },
        {
            "id": "manual_002_ac_xlsx",
            "input_file": "Endtestdaten_ohne_Fehler_ einheitliche ID/epaAC-Data-4.xlsx",
            "expected_table": "tbImportAcData",
            "mapping": {
                "E2_I_226": "coE2I226",
                "E2_I_222 (E3_I_0889)(STRING)": "coE2I222",
                "E2_I_225": "coE2I225",
                "E0_I_001": "coE0I001",
                "E0_I_002": "coE0I002",
                "E0_I_003": "coE0I003",
            },
            "output_columns": ["coE2I226", "coE2I222", "coE2I225", "coE0I001", "coE0I002"],
        },
        {
            "id": "manual_003_ac_clinic4_xlsx",
            "input_file": "Endtestdaten_ohne_Fehler_ einheitliche ID/split_data_pat_case_altered/split_data_pat_case_altered/clinic_4_epaAC-Data.xlsx",
            "expected_table": "tbImportAcData",
            "mapping": {
                "E2_I_226": "coE2I226",
                "E2_I_222 (E3_I_0889)(STRING)": "coE2I222",
                "E2_I_225": "coE2I225",
                "E0_I_001": "coE0I001",
                "E0_I_002": "coE0I002",
                "E0_I_003": "coE0I003",
            },
            "output_columns": ["coE2I226", "coE2I222", "coE2I225", "coE0I001", "coE0I002"],
        },
        {
            "id": "manual_004_labs_csv",
            "input_file": "Endtestdaten_mit_Fehlern_ einheitliche ID/synth_labs.csv",
            "expected_table": "tbImportLabsData",
            "mapping": {
                "CaseID": "coCaseId",
                "SpecDT": "coSpecimen_datetime",
                "Na": "coSodium_mmol_L",
                "K": "coPotassium_mmol_L",
                "Creat": "coCreatinine_mg_dL",
                "PID": None,
                "Gender": None,
                "Age": None,
            },
            "output_columns": [
                "coCaseId",
                "coSpecimen_datetime",
                "coSodium_mmol_L",
                "coPotassium_mmol_L",
                "coCreatinine_mg_dL",
            ],
        },
        {
            "id": "manual_005_labs_xlsx",
            "input_file": "Endtestdaten_mit_Fehlern_ einheitliche ID/synth_labs.xlsx",
            "expected_table": "tbImportLabsData",
            "mapping": {
                "CaseID": "coCaseId",
                "SpecDT": "coSpecimen_datetime",
                "Na": "coSodium_mmol_L",
                "K": "coPotassium_mmol_L",
                "Creat": "coCreatinine_mg_dL",
                "PID": None,
                "Gender": None,
                "Age": None,
            },
            "output_columns": [
                "coCaseId",
                "coSpecimen_datetime",
                "coSodium_mmol_L",
                "coPotassium_mmol_L",
                "coCreatinine_mg_dL",
            ],
        },
        {
            "id": "manual_006_med_csv",
            "input_file": "Endtestdaten_mit_Fehlern_ einheitliche ID/synth_medication_raw_inpatient.csv",
            "expected_table": "tbImportMedicationInpatientData",
            "mapping": {
                "rec_type": "coRecord_type",
                "pat_id": "coPatient_id",
                "enc_id": "coEncounter_id",
                "station": "coWard",
                "aufnahme_dt": "coAdmission_datetime",
                "atc_code": "coMedication_code_atc",
                "dosis": "coDose",
            },
            "output_columns": [
                "coRecord_type",
                "coPatient_id",
                "coEncounter_id",
                "coWard",
                "coAdmission_datetime",
                "coMedication_code_atc",
                "coDose",
            ],
        },
        {
            "id": "manual_007_med_xlsx",
            "input_file": "Endtestdaten_mit_Fehlern_ einheitliche ID/synth_medication_raw_inpatient.xlsx",
            "expected_table": "tbImportMedicationInpatientData",
            "mapping": {
                "rec_type": "coRecord_type",
                "pat_id": "coPatient_id",
                "enc_id": "coEncounter_id",
                "station": "coWard",
                "aufnahme_dt": "coAdmission_datetime",
                "atc_code": "coMedication_code_atc",
                "dosis": "coDose",
            },
            "output_columns": [
                "coRecord_type",
                "coPatient_id",
                "coEncounter_id",
                "coWard",
                "coAdmission_datetime",
                "coMedication_code_atc",
                "coDose",
            ],
        },
        {
            "id": "manual_008_device_csv",
            "input_file": "Endtestdaten_mit_Fehlern_ einheitliche ID/synth_device_motion_fall.csv",
            "expected_table": "tbImportDeviceMotionData",
            "mapping": {
                "patient_id": "coPatient_id",
                "timestamp": "coTimestamp",
                "movement_index_0_100": "coMovement_index_0_100",
                "fall_event_0_1": "coFall_event_0_1",
                "impact_magnitude_g": "coImpact_magnitude_g",
            },
            "output_columns": [
                "coPatient_id",
                "coTimestamp",
                "coMovement_index_0_100",
                "coFall_event_0_1",
                "coImpact_magnitude_g",
            ],
        },
        {
            "id": "manual_009_device_xlsx",
            "input_file": "Endtestdaten_mit_Fehlern_ einheitliche ID/synth_device_motion_fall.xlsx",
            "expected_table": "tbImportDeviceMotionData",
            "mapping": {
                "patient_id": "coPatient_id",
                "timestamp": "coTimestamp",
                "movement_index_0_100": "coMovement_index_0_100",
                "fall_event_0_1": "coFall_event_0_1",
                "impact_magnitude_g": "coImpact_magnitude_g",
            },
            "output_columns": [
                "coPatient_id",
                "coTimestamp",
                "coMovement_index_0_100",
                "coFall_event_0_1",
                "coImpact_magnitude_g",
            ],
        },
        {
            "id": "manual_010_device1hz_csv",
            "input_file": "Endtestdaten_mit_Fehlern_ einheitliche ID/synth_device_raw_1hz_motion_fall.csv",
            "expected_table": "tbImportDevice1HzMotionData",
            "mapping": {
                "PatientID": "coPatient_id",
                "DeviceID": "coDevice_id",
                "Timestamp": "coTimestamp",
                "MovementScore": "coMovement_score_0_100",
                "FallEvent": "coFall_event_0_1",
                "EventID": "coEvent_id",
                "col_19": None,
            },
            "output_columns": [
                "coPatient_id",
                "coDevice_id",
                "coTimestamp",
                "coMovement_score_0_100",
                "coFall_event_0_1",
                "coEvent_id",
            ],
        },
        {
            "id": "manual_011_icd_csv",
            "input_file": "Endtestdaten_mit_Fehlern_ einheitliche ID/synth_cases_icd10_ops.csv",
            "expected_table": "tbImportIcd10Data",
            "mapping": {
                "CaseID": "coCaseId",
                "Station": "coWard",
                "Aufnahmedatum": "coAdmission_date",
                "Entlassungsdatum": "coDischarge_date",
                "ICD10_Haupt": "coPrimary_icd10_code",
                "OPS_Code": "coOps_codes",
            },
            "output_columns": [
                "coCaseId",
                "coWard",
                "coAdmission_date",
                "coDischarge_date",
                "coPrimary_icd10_code",
                "coOps_codes",
            ],
        },
        {
            "id": "manual_012_icd_xlsx",
            "input_file": "Endtestdaten_mit_Fehlern_ einheitliche ID/synth_cases_icd10_ops.xlsx",
            "expected_table": "tbImportIcd10Data",
            "mapping": {
                "CaseID": "coCaseId",
                "Station": "coWard",
                "Aufnahmedatum": "coAdmission_date",
                "Entlassungsdatum": "coDischarge_date",
                "ICD10_Haupt": "coPrimary_icd10_code",
                "OPS_Code": "coOps_codes",
                "col_13": None,
                "col_14": None,
                "col_15": None,
                "col_16": None,
                "col_17": None,
                "col_18": None,
            },
            "output_columns": [
                "coCaseId",
                "coWard",
                "coAdmission_date",
                "coDischarge_date",
                "coPrimary_icd10_code",
                "coOps_codes",
            ],
        },
        {
            "id": "manual_013_icd_clinic2_csv",
            "input_file": "Endtestdaten_ohne_Fehler_ einheitliche ID/split_data_pat_case_altered/split_data_pat_case_altered/clinic_2_icd_ops.csv",
            "expected_table": "tbImportIcd10Data",
            "mapping": {
                "ID_CAS": "coCaseId",
                "WARD": "coWard",
                "DATE_AD": "coAdmission_date",
                "DATE_DIS": "coDischarge_date",
                "D_S": "coPrimary_icd10_code",
                "PROC": "coOps_codes",
            },
            "output_columns": [
                "coCaseId",
                "coWard",
                "coAdmission_date",
                "coDischarge_date",
                "coPrimary_icd10_code",
                "coOps_codes",
            ],
        },
        {
            "id": "manual_014_nursing_csv",
            "input_file": "Endtestdaten_mit_Fehlern_ einheitliche ID/synth_nursing_daily_reports.csv",
            "expected_table": "tbImportNursingDailyReportsData",
            "mapping": {
                "CaseID": "coCaseId",
                "PatientID": "coPatient_id",
                "Ward": "coWard",
                "ReportDate": "coReport_date",
                "Shift": "coShift",
                "NursingNote": "coNursing_note_free_text",
            },
            "output_columns": [
                "coCaseId",
                "coPatient_id",
                "coWard",
                "coReport_date",
                "coShift",
                "coNursing_note_free_text",
            ],
        },
        {
            "id": "manual_015_nursing_pdf",
            "input_file": "Endtestdaten_ohne_Fehler_ einheitliche ID/split_data_pat_case_altered/split_data_pat_case_altered/clinic_4_nursing.pdf",
            "expected_table": "tbImportNursingDailyReportsData",
            "mapping": {
                "case_id": "coCaseId",
                "patient_id": "coPatient_id",
                "ward": "coWard",
                "report_date": "coReport_date",
                "shift": "coShift",
                "nursing_note_free_text": "coNursing_note_free_text",
            },
            "output_columns": [
                "coCaseId",
                "coPatient_id",
                "coWard",
                "coReport_date",
                "coShift",
                "coNursing_note_free_text",
            ],
        },
    ]

    manifest_cases: list[dict[str, Any]] = []
    for case in cases:
        input_path = root / case["input_file"]
        table = case["expected_table"]
        headers, records, _ = std.load_records(input_path, table)

        # Validate mapping keys exist in source headers.
        mapping = case["mapping"]
        missing = [k for k in mapping.keys() if k not in headers]
        if missing:
            raise ValueError(f"{case['id']}: mapping keys not in headers: {missing}")

        expected_rows = build_rows(
            records=records,
            mapping=mapping,
            output_columns=case["output_columns"],
            max_rows=args.max_rows,
        )

        mapping_path = exp_map_dir / f"{case['id']}.mapping.json"
        output_path = exp_out_dir / f"{case['id']}.expected.csv"
        mapping_path.write_text(
            json.dumps(mapping, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        write_expected_csv(output_path, expected_rows, case["output_columns"])

        manifest_cases.append(
            {
                "id": case["id"],
                "input_file": case["input_file"],
                "expected_table": table,
                "expected_mapping_file": to_rel(mapping_path, root),
                "expected_output_file": to_rel(output_path, root),
            }
        )

    manifest = {
        "meta": {
            "kind": "manual_gold_subset",
            "description": "Manually curated 15-case benchmark with explicit source->target mappings and expected output slices.",
            "cases": len(manifest_cases),
            "max_expected_rows_per_case": args.max_rows,
        },
        "cases": manifest_cases,
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {manifest_path}")
    print(f"cases {len(manifest_cases)}")


if __name__ == "__main__":
    main()
