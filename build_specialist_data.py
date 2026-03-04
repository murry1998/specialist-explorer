#!/usr/bin/env python3
"""
Unified data pipeline for Specialist Explorer.

Fetches all specialists (Neurology, Nephrology, Rheumatology, Urology) from
CMS Medicare data, flags CCM billing status, enriches with facility names,
classifies practice type, and outputs a single CSV + zip coordinate lookup.

Data sources:
  - "by Provider" (one row per NPI):
    https://data.cms.gov/data-api/v1/dataset/8889d81e-2ee7-448f-8713-f071038289b5/data
  - "by Provider and Service" (one row per NPI x HCPCS):
    https://data.cms.gov/data-api/v1/dataset/92396110-2aed-4d63-a6a2-5d6207d46a29/data
  - "Doctors and Clinicians" (DAC, facility names):
    https://data.cms.gov/provider-data/api/1/datastore/query/mj5m-pzi6/0
Year: CY 2023

Output:
  public/data/specialists.csv
  public/data/zip_coords.json
"""

import requests
import csv
import json
import time
import os
import sys

# ── API Endpoints ────────────────────────────────────────────────────────
BY_PROVIDER_URL = (
    "https://data.cms.gov/data-api/v1/dataset/"
    "8889d81e-2ee7-448f-8713-f071038289b5/data"
)
BY_PROVIDER_AND_SERVICE_URL = (
    "https://data.cms.gov/data-api/v1/dataset/"
    "92396110-2aed-4d63-a6a2-5d6207d46a29/data"
)
DAC_API_URL = (
    "https://data.cms.gov/provider-data/api/1/datastore/query/mj5m-pzi6/0"
)

PAGE_SIZE = 5000
DAC_PAGE_SIZE = 1000

TARGET_SPECIALTIES = ["Neurology", "Nephrology", "Rheumatology", "Urology"]

CCM_CODES = ["99490", "99491", "99487", "99489", "99437", "99439"]

HEALTH_SYSTEM_THRESHOLD = 10  # providers per facility name

# NP/PA credential patterns
NP_PA_PATTERNS = [
    "PA", "NP", "PA-C", "PAC", "FNP", "CFNP", "ARNP", "DNP", "CNP",
    "ACNP", "ANP", "CRNP", "FNP-C", "FNP-BC", "AGNP", "GNP",
    "NP-C", "PMHNP", "WHNP", "PNP", "CNS", "APRN",
    "RPA", "RPA-C",
]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "public", "data")


def classify_provider(credentials):
    """Classify provider as 'Doctor' or 'NP/PA' based on credentials."""
    if not credentials or not credentials.strip():
        return "Doctor"
    raw = credentials.strip().upper()
    stripped = raw.replace(".", "").replace(",", "").replace(" ", "")

    if stripped in ("MD", "DO", "MDPHD", "MDMPH", "DOPHD", "DOMPH"):
        return "Doctor"

    intl_patterns = ("MBBS", "MBBCH", "MBCHB", "MBBCHBAO", "MBCH", "MB")
    for p in intl_patterns:
        if stripped.startswith(p):
            return "Doctor"

    if "MD" in stripped or "DO" in stripped:
        return "Doctor"

    for pattern in NP_PA_PATTERNS:
        if pattern in stripped:
            return "NP/PA"

    if stripped == "DR":
        return "Doctor"

    return "Other"


def classify_setting(ruca_code):
    """Classify RUCA code into Urban / Suburban / Rural."""
    if not ruca_code:
        return ""
    try:
        code = float(ruca_code)
    except (ValueError, TypeError):
        return ""
    if code <= 3:
        return "Urban"
    elif code <= 6:
        return "Suburban"
    else:
        return "Rural"


def fetch_paginated(base_url, params_base, label=""):
    """Fetch all pages from a CMS API endpoint."""
    records = []
    offset = 0

    while True:
        params = {**params_base, "size": PAGE_SIZE, "offset": offset}
        try:
            resp = requests.get(base_url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            print(f"  Error at offset {offset}: {e} — retrying in 5s...")
            time.sleep(5)
            try:
                resp = requests.get(base_url, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except requests.exceptions.RequestException as e2:
                print(f"  Retry failed: {e2}. Stopping pagination for {label}.")
                break

        if not data:
            break

        records.extend(data)
        fetched = len(data)
        print(f"  {label}: offset={offset}, fetched={fetched}, total={len(records)}")

        if fetched < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(0.5)

    return records


def fetch_dac_by_specialty(specialty):
    """Fetch all DAC records for a given primary specialty."""
    records = []
    offset = 0

    while True:
        params = {
            "conditions[0][property]": "pri_spec",
            "conditions[0][value]": specialty,
            "conditions[0][operator]": "=",
            "limit": DAC_PAGE_SIZE,
            "offset": offset,
        }
        try:
            resp = requests.get(DAC_API_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            print(f"  Error at offset {offset}: {e} — retrying in 5s...")
            time.sleep(5)
            try:
                resp = requests.get(DAC_API_URL, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except requests.exceptions.RequestException as e2:
                print(f"  Retry failed: {e2}. Stopping.")
                break

        results = data.get("results", [])
        if not results:
            break

        records.extend(results)
        fetched = len(results)
        print(f"  {specialty}: offset={offset}, fetched={fetched}, total={len(records)}")

        if fetched < DAC_PAGE_SIZE:
            break

        offset += DAC_PAGE_SIZE
        time.sleep(0.3)

    return records


def normalize_addr(street, city, state, zip5):
    """Normalize address for matching."""
    return (
        (street or "").strip().upper(),
        (city or "").strip().upper(),
        (state or "").strip().upper(),
        (zip5 or "").strip()[:5],
    )


def main():
    print("=" * 70)
    print("Specialist Explorer — Unified Data Pipeline")
    print("Specialties: Neurology, Nephrology, Rheumatology, Urology")
    print("Data: CMS Medicare Physician & Other Practitioners, CY 2023")
    print("=" * 70)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ══════════════════════════════════════════════════════════════════════
    # STEP 1: Fetch all providers in target specialties
    # ══════════════════════════════════════════════════════════════════════
    print("\n[Step 1/6] Fetching all providers in target specialties...")
    all_providers = {}  # NPI → provider record

    for specialty in TARGET_SPECIALTIES:
        print(f"\n  Fetching {specialty}...")
        records = fetch_paginated(
            BY_PROVIDER_URL,
            {"filter[Rndrng_Prvdr_Type]": specialty},
            label=specialty,
        )
        for rec in records:
            npi = rec.get("Rndrng_NPI", "")
            if npi:
                all_providers[npi] = rec
        print(f"  {specialty}: {len(records)} records fetched")

    print(f"\n  Total unique providers: {len(all_providers):,}")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 2: Identify CCM-billing NPIs (for flagging, NOT exclusion)
    # ══════════════════════════════════════════════════════════════════════
    print("\n[Step 2/6] Identifying CCM-billing providers...")
    ccm_npis = set()

    for specialty in TARGET_SPECIALTIES:
        for code in CCM_CODES:
            label = f"{specialty}/{code}"
            records = fetch_paginated(
                BY_PROVIDER_AND_SERVICE_URL,
                {
                    "filter[Rndrng_Prvdr_Type]": specialty,
                    "filter[HCPCS_Cd]": code,
                },
                label=label,
            )
            for rec in records:
                npi = rec.get("Rndrng_NPI", "")
                if npi:
                    ccm_npis.add(npi)

    print(f"\n  CCM-billing providers found: {len(ccm_npis):,}")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 3: Build output rows — ALL providers with enriched fields
    # ══════════════════════════════════════════════════════════════════════
    print("\n[Step 3/6] Building provider rows with enriched data...")

    rows = []
    for npi, rec in all_providers.items():
        creds = rec.get("Rndrng_Prvdr_Crdntls", "")
        entity_cd = rec.get("Rndrng_Prvdr_Ent_Cd", "")
        ruca = rec.get("Rndrng_Prvdr_RUCA", "")
        tot_benes = rec.get("Tot_Benes", "")
        tot_srvcs = rec.get("Tot_Srvcs", "")

        rows.append({
            "NPI": npi,
            "First_Name": rec.get("Rndrng_Prvdr_First_Name", ""),
            "Last_Name": rec.get("Rndrng_Prvdr_Last_Org_Name", ""),
            "Credentials": creds,
            "Provider_Type": classify_provider(creds),
            "Specialty": rec.get("Rndrng_Prvdr_Type", ""),
            "Bills_CCM": "Yes" if npi in ccm_npis else "No",
            "Practice_Type": "",  # filled in Step 5
            "Entity_Type": "Organization" if entity_cd == "O" else "Individual",
            "Setting": classify_setting(ruca),
            "Tot_Benes": tot_benes if tot_benes else "0",
            "Tot_Srvcs": tot_srvcs if tot_srvcs else "0",
            "Facility_Name": "",  # filled in Step 4
            "Street_1": rec.get("Rndrng_Prvdr_St1", ""),
            "Street_2": rec.get("Rndrng_Prvdr_St2", ""),
            "City": rec.get("Rndrng_Prvdr_City", ""),
            "State": rec.get("Rndrng_Prvdr_State_Abrvtn", ""),
            "Zip": rec.get("Rndrng_Prvdr_Zip5", ""),
        })

    # Count providers per billing address
    addr_counts = {}
    for row in rows:
        key = (row["Street_1"].lower().strip(),
               row["City"].lower().strip(),
               row["State"].strip(),
               row["Zip"].strip())
        addr_counts[key] = addr_counts.get(key, 0) + 1

    for row in rows:
        key = (row["Street_1"].lower().strip(),
               row["City"].lower().strip(),
               row["State"].strip(),
               row["Zip"].strip())
        row["Providers_At_Address"] = addr_counts[key]

    rows.sort(key=lambda r: (r["State"], r["Last_Name"], r["First_Name"]))

    print(f"  Total rows: {len(rows):,}")
    ccm_count = sum(1 for r in rows if r["Bills_CCM"] == "Yes")
    print(f"  Bills CCM: {ccm_count:,}")
    print(f"  Non-CCM:   {len(rows) - ccm_count:,}")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 4: Fetch facility names from DAC dataset
    # ══════════════════════════════════════════════════════════════════════
    print("\n[Step 4/6] Fetching facility names from DAC dataset...")
    all_dac_records = []

    for specialty in TARGET_SPECIALTIES:
        print(f"\n  Fetching {specialty}...")
        records = fetch_dac_by_specialty(specialty)
        all_dac_records.extend(records)
        print(f"  {specialty}: {len(records):,} total records")

    print(f"\n  Total DAC records fetched: {len(all_dac_records):,}")

    # Build NPI → facility_name mapping (prefer match by billing address)
    npi_facilities = {}
    for rec in all_dac_records:
        npi = rec.get("npi", "")
        fname = (rec.get("facility_name") or "").strip()
        if not npi or not fname:
            continue
        addr_key = normalize_addr(
            rec.get("adr_ln_1", ""),
            rec.get("citytown", ""),
            rec.get("state", ""),
            rec.get("zip_code", ""),
        )
        if npi not in npi_facilities:
            npi_facilities[npi] = []
        npi_facilities[npi].append({"addr": addr_key, "facility": fname})

    # Build address → facility_name map (most common name at each address)
    addr_facility_counts = {}
    for rec in all_dac_records:
        fname = (rec.get("facility_name") or "").strip()
        if not fname:
            continue
        addr_key = normalize_addr(
            rec.get("adr_ln_1", ""),
            rec.get("citytown", ""),
            rec.get("state", ""),
            rec.get("zip_code", ""),
        )
        if addr_key not in addr_facility_counts:
            addr_facility_counts[addr_key] = {}
        addr_facility_counts[addr_key][fname] = addr_facility_counts[addr_key].get(fname, 0) + 1

    addr_to_facility = {}
    for addr_key, fnames in addr_facility_counts.items():
        addr_to_facility[addr_key] = max(fnames, key=fnames.get)

    # Merge facility names into rows
    matched_npi_addr = 0
    matched_npi_only = 0
    matched_addr = 0
    not_matched = 0

    for row in rows:
        npi = row["NPI"]
        row_addr = normalize_addr(row["Street_1"], row["City"], row["State"], row["Zip"])
        facility = ""

        # Strategy 1: NPI + address
        if npi in npi_facilities:
            for entry in npi_facilities[npi]:
                if entry["addr"] == row_addr:
                    facility = entry["facility"]
                    matched_npi_addr += 1
                    break
            # Strategy 2: NPI only
            if not facility:
                facility = npi_facilities[npi][0]["facility"]
                matched_npi_only += 1

        # Strategy 3: Address only
        if not facility and row_addr in addr_to_facility:
            facility = addr_to_facility[row_addr]
            matched_addr += 1

        if not facility:
            not_matched += 1

        row["Facility_Name"] = facility

    total_matched = matched_npi_addr + matched_npi_only + matched_addr
    print(f"\n  Facility name matching:")
    print(f"    By NPI + address: {matched_npi_addr:,}")
    print(f"    By NPI only:      {matched_npi_only:,}")
    print(f"    By address:       {matched_addr:,}")
    print(f"    Not matched:      {not_matched:,}")
    print(f"    Coverage: {total_matched:,}/{len(rows):,} ({100*total_matched/len(rows):.1f}%)")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 5: Classify Practice_Type (Health System vs Independent)
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[Step 5/6] Classifying practice type (threshold: {HEALTH_SYSTEM_THRESHOLD}+ providers)...")

    # Count ALL providers per facility name (not just doctors)
    facility_counts = {}
    for row in rows:
        fn = row["Facility_Name"].strip()
        if fn:
            facility_counts[fn] = facility_counts.get(fn, 0) + 1

    health_system_count = 0
    independent_count = 0
    for row in rows:
        fn = row["Facility_Name"].strip()
        if fn and facility_counts.get(fn, 0) >= HEALTH_SYSTEM_THRESHOLD:
            row["Practice_Type"] = "Health System"
            health_system_count += 1
        else:
            row["Practice_Type"] = "Independent"
            independent_count += 1

    print(f"  Health System: {health_system_count:,} ({100*health_system_count/len(rows):.1f}%)")
    print(f"  Independent:   {independent_count:,} ({100*independent_count/len(rows):.1f}%)")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 6: Write output files
    # ══════════════════════════════════════════════════════════════════════
    print("\n[Step 6/6] Writing output files...")

    fieldnames = [
        "NPI", "First_Name", "Last_Name", "Credentials", "Provider_Type",
        "Specialty", "Bills_CCM", "Practice_Type", "Entity_Type", "Setting",
        "Tot_Benes", "Tot_Srvcs",
        "Facility_Name", "Street_1", "Street_2", "City", "State", "Zip",
        "Providers_At_Address",
    ]

    csv_path = os.path.join(OUTPUT_DIR, "specialists.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved CSV: {csv_path}")

    # Generate zip coordinates
    generate_zip_coords(rows, OUTPUT_DIR)

    # ── Summary ──────────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    print(f"Total providers: {len(rows):,}")
    print(f"Bills CCM: {ccm_count:,} | Non-CCM: {len(rows) - ccm_count:,}")
    print(f"Health System: {health_system_count:,} | Independent: {independent_count:,}")

    print(f"\nBy specialty:")
    spec_counts = {}
    for r in rows:
        spec_counts[r["Specialty"]] = spec_counts.get(r["Specialty"], 0) + 1
    for spec, count in sorted(spec_counts.items()):
        print(f"  {spec}: {count:,}")

    print(f"\nBy provider type:")
    type_counts = {}
    for r in rows:
        type_counts[r["Provider_Type"]] = type_counts.get(r["Provider_Type"], 0) + 1
    for ptype, count in sorted(type_counts.items()):
        print(f"  {ptype}: {count:,}")

    print(f"\nBy setting:")
    setting_counts = {}
    for r in rows:
        s = r["Setting"] or "(unknown)"
        setting_counts[s] = setting_counts.get(s, 0) + 1
    for setting, count in sorted(setting_counts.items(), key=lambda x: -x[1]):
        print(f"  {setting}: {count:,}")

    print(f"\nBy entity type:")
    ent_counts = {}
    for r in rows:
        ent_counts[r["Entity_Type"]] = ent_counts.get(r["Entity_Type"], 0) + 1
    for ent, count in sorted(ent_counts.items()):
        print(f"  {ent}: {count:,}")

    print(f"\nTop 15 states:")
    state_counts = {}
    for r in rows:
        state_counts[r["State"]] = state_counts.get(r["State"], 0) + 1
    for state, count in sorted(state_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"  {state}: {count:,}")


def generate_zip_coords(rows, output_dir):
    """Generate a zip → {lat, lng} JSON file using pgeocode."""
    try:
        import pgeocode
        nomi = pgeocode.Nominatim("us")

        # Generate ALL US zip codes, not just dataset zips
        print("  Generating comprehensive US zip coordinate lookup...")
        import numpy as np

        # Query all 5-digit zip codes (00000-99999 filtered by validity)
        all_zips = [str(z).zfill(5) for z in range(100000)]
        coords = {}
        batch_size = 500
        for i in range(0, len(all_zips), batch_size):
            batch = all_zips[i:i + batch_size]
            results = nomi.query_postal_code(batch)
            if hasattr(results, "iterrows"):
                for _, row in results.iterrows():
                    pc = str(row.get("postal_code", "")).zfill(5)
                    lat = row.get("latitude")
                    lng = row.get("longitude")
                    if lat == lat and lng == lng and lat is not None:  # NaN check
                        coords[pc] = {
                            "lat": round(float(lat), 4),
                            "lng": round(float(lng), 4),
                        }
            if (i // batch_size) % 40 == 0 and i > 0:
                print(f"    Processed {min(i + batch_size, 100000)} zip codes... ({len(coords)} valid)")

        out_path = os.path.join(output_dir, "zip_coords.json")
        with open(out_path, "w") as f:
            json.dump(coords, f, separators=(",", ":"))
        print(f"  Saved zip coordinates: {out_path} ({len(coords):,} zips)")

    except ImportError:
        print("  WARNING: pgeocode not installed. Install with: pip install pgeocode")
        print("  Using fallback method...")
        generate_zip_coords_fallback(rows, output_dir)


def generate_zip_coords_fallback(rows, output_dir):
    """Fallback: download a free zip code database from the web."""
    url = "https://raw.githubusercontent.com/scpike/us-state-county-zip/master/geo-data.csv"
    print("  Downloading zip code data from GitHub...")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        lines = resp.text.strip().split("\n")
        coords = {}
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) >= 4:
                zipcode = parts[0].strip().zfill(5)
                try:
                    lat = round(float(parts[1]), 4)
                    lng = round(float(parts[2]), 4)
                    coords[zipcode] = {"lat": lat, "lng": lng}
                except ValueError:
                    pass

        out_path = os.path.join(output_dir, "zip_coords.json")
        with open(out_path, "w") as f:
            json.dump(coords, f, separators=(",", ":"))
        print(f"  Saved zip coordinates: {out_path} ({len(coords)} zips)")
    except Exception as e:
        print(f"  ERROR generating zip coords: {e}")
        out_path = os.path.join(output_dir, "zip_coords.json")
        with open(out_path, "w") as f:
            f.write("{}")
        print("  Wrote empty zip_coords.json — distance feature will be unavailable")


if __name__ == "__main__":
    main()
