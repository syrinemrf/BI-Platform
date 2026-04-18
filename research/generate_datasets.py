#!/usr/bin/env python3
"""
Generate 4 synthetic datasets for the LLM-powered ETL research.
Each dataset has intentional data quality issues for testing.
"""
import csv
import json
import random
import string
import uuid
import os
from datetime import datetime, timedelta
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom.minidom import parseString

random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data", "raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─── Helpers ───────────────────────────────────────────────
FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace",
               "Hank", "Ivy", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia",
               "Paul", "Quinn", "Rose", "Sam", "Tina", "Uma", "Victor",
               "Wendy", "Xander", "Yara", "Zane"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
              "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
              "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
              "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris"]
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
          "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
          "London", "Paris", "Berlin", "Tokyo", "Sydney", "Toronto", "Mumbai"]
COUNTRIES_CLEAN = ["USA", "UK", "France", "Germany", "Japan", "Australia", "Canada", "India"]
COUNTRIES_USA_VARIANTS = ["USA", "United States", "US", "united states"]
PRODUCT_CATEGORIES = {
    "Electronics": ["Laptop", "Smartphone", "Tablet", "Headphones", "Monitor", "Keyboard"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Hat", "Dress"],
    "Home": ["Lamp", "Pillow", "Rug", "Vase", "Clock", "Frame"],
    "Food": ["Coffee", "Tea", "Chocolate", "Pasta", "Olive Oil", "Honey"],
}
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Wire Transfer", "Cash"]
DELIVERY_STATUSES = ["Delivered", "Shipped", "Processing", "Returned", "Cancelled"]
REGIONS = ["North", "South", "East", "West", "Central"]
DATE_FORMATS = [
    lambda d: d.strftime("%Y-%m-%d"),
    lambda d: d.strftime("%d/%m/%Y"),
    lambda d: d.strftime("%b %d %Y"),
]


def rand_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def rand_email(name):
    parts = name.lower().split()
    domain = random.choice(["gmail.com", "yahoo.com", "outlook.com", "company.com"])
    return f"{parts[0]}.{parts[1]}@{domain}"


def rand_date(start_year=2023, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


# ═══════════════════════════════════════════════════════════
# DATASET 1 — Retail Sales CSV
# ═══════════════════════════════════════════════════════════
def generate_dataset1():
    rows = []
    order_ids = list(range(10001, 10501))
    # Create 3% duplicates
    num_dupes = int(500 * 0.03)
    for _ in range(num_dupes):
        idx = random.randint(0, len(order_ids) - 1)
        order_ids.append(order_ids[idx])
    random.shuffle(order_ids)

    for i, oid in enumerate(order_ids):
        name = rand_name()
        email = rand_email(name) if random.random() > 0.08 else ""
        city = random.choice(CITIES)

        # Inconsistent country
        if random.random() < 0.3:
            country = random.choice(COUNTRIES_USA_VARIANTS)
        else:
            country = random.choice(COUNTRIES_CLEAN)

        cat = random.choice(list(PRODUCT_CATEGORIES.keys()))
        product = random.choice(PRODUCT_CATEGORIES[cat])

        unit_price = round(random.uniform(5.0, 500.0), 2)
        # 5% with currency symbol
        if random.random() < 0.05:
            unit_price_str = f"${unit_price}"
        else:
            unit_price_str = str(unit_price)

        quantity = random.randint(1, 20)
        # 1% negative quantity
        if random.random() < 0.01:
            quantity = -abs(quantity)

        discount = round(random.uniform(0, 0.3), 2) if random.random() > 0.05 else ""

        total = round(unit_price * quantity * (1 - (float(discount) if discount != "" else 0)), 2)

        # Mixed date formats
        d = rand_date()
        fmt_fn = random.choice(DATE_FORMATS)
        date_str = fmt_fn(d)

        rows.append({
            "order_id": oid,
            "order_date": date_str,
            "customer_name": name,
            "customer_email": email,
            "customer_city": city,
            "customer_country": country,
            "product_name": product,
            "product_category": cat,
            "product_subcategory": f"{cat}-{product}",
            "unit_price": unit_price_str,
            "quantity": quantity,
            "discount_pct": discount,
            "total_amount": total,
            "payment_method": random.choice(PAYMENT_METHODS),
            "delivery_status": random.choice(DELIVERY_STATUSES),
            "sales_rep_name": rand_name(),
            "region": random.choice(REGIONS),
        })

    path = os.path.join(OUTPUT_DIR, "dataset1_retail_sales.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows[:500])  # exactly 500 rows
    print(f"  ✓ dataset1_retail_sales.csv — {min(len(rows), 500)} rows")


# ═══════════════════════════════════════════════════════════
# DATASET 2 — Hospital Records JSON
# ═══════════════════════════════════════════════════════════
BLOOD_TYPES = ["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"]
GENDERS_VARIANTS = ["M", "Male", "m", "MALE", "F", "Female", "f", "FEMALE"]
DEPARTMENTS = ["Emergency", "Cardiology", "Orthopedics", "Neurology",
               "Oncology", "Pediatrics", "General Surgery", "Internal Medicine"]
DIAGNOSIS_CODES = [
    ("I21", "Acute myocardial infarction"),
    ("J18", "Pneumonia, unspecified organism"),
    ("S72", "Fracture of femur"),
    ("K35", "Acute appendicitis"),
    ("G40", "Epilepsy"),
    ("E11", "Type 2 diabetes mellitus"),
    ("N18", "Chronic kidney disease"),
    ("C34", "Malignant neoplasm of bronchus"),
]
SEVERITY_VARIANTS = ["mild", "Mild", "MILD", "moderate", "Moderate", "MODERATE",
                     "medium", "severe", "Severe", "SEVERE", "critical"]
OUTCOMES = ["Recovered", "Improved", "Transferred", "Deceased", "Under Treatment"]


def generate_dataset2():
    records = []
    for i in range(300):
        pid = f"PAT-{10000 + i}"
        adm_date = rand_date(2023, 2024)

        # 5% missing discharge
        if random.random() < 0.05:
            dis_date = None
        else:
            days = random.randint(1, 30)
            dis_date = adm_date + timedelta(days=days)
            # 3% discharge before admission
            if random.random() < 0.03:
                dis_date = adm_date - timedelta(days=random.randint(1, 5))

        name = rand_name()
        dob = rand_date(1940, 2005)
        gender = random.choice(GENDERS_VARIANTS)
        blood = random.choice(BLOOD_TYPES)
        phone = f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}" if random.random() > 0.10 else None
        email = rand_email(name)

        diag = random.choice(DIAGNOSIS_CODES)
        severity = random.choice(SEVERITY_VARIANTS)

        dept = random.choice(DEPARTMENTS)
        doctor = f"Dr. {rand_name()}"

        total_cost = round(random.uniform(500, 50000), 2)
        insurance_pct = random.uniform(0.5, 0.95)
        insurance_covered = round(total_cost * insurance_pct, 2)
        patient_paid = round(total_cost - insurance_covered, 2)

        # 4% cost mismatch
        if random.random() < 0.04:
            insurance_covered = round(insurance_covered + random.uniform(50, 500), 2)

        record = {
            "patient_id": pid,
            "admission_date": adm_date.strftime("%Y-%m-%d"),
            "discharge_date": dis_date.strftime("%Y-%m-%d") if dis_date else None,
            "patient": {
                "full_name": name,
                "date_of_birth": dob.strftime("%Y-%m-%d"),
                "gender": gender,
                "blood_type": blood,
                "contact": {
                    "phone": phone,
                    "email": email,
                }
            },
            "diagnosis": {
                "primary_code": diag[0],
                "primary_label": diag[1],
                "severity": severity,
            },
            "treatment": {
                "department": dept,
                "attending_doctor": doctor,
                "total_cost": total_cost,
                "insurance_covered": insurance_covered,
                "patient_paid": patient_paid,
            },
            "outcome": random.choice(OUTCOMES),
        }
        records.append(record)

    path = os.path.join(OUTPUT_DIR, "dataset2_hospital_records.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    print(f"  ✓ dataset2_hospital_records.json — {len(records)} records")


# ═══════════════════════════════════════════════════════════
# DATASET 3 — Supplier Invoices XML
# ═══════════════════════════════════════════════════════════
SUPPLIER_CATEGORIES = ["Raw Materials", "IT Services", "Logistics",
                       "Office Supplies", "Consulting", "Manufacturing"]
CURRENCIES = ["EUR", "USD", "GBP"]
STATUS_VARIANTS = ["paid", "PAID", "Paid", "settled", "pending", "Pending", "overdue"]
ITEM_DESCRIPTIONS = ["Widget A", "Component B", "Service Fee", "License",
                     "Transport", "Packaging", "Consulting Hours", "Raw Material X",
                     "Maintenance", "Software Subscription"]
BUYER_DEPTS = [
    ("FIN-01", "Finance", "CC-100"),
    ("ENG-02", "Engineering", "CC-200"),
    ("MKT-03", "Marketing", "CC-300"),
    ("OPS-04", "Operations", "CC-400"),
    ("HR-05", "Human Resources", "CC-500"),
]
SUPPLIER_COUNTRIES = ["France", "Germany", "USA", "China", "India", "UK", "Japan"]


def generate_dataset3():
    root = Element("invoices")
    invoice_ids = list(range(2001, 2201))
    # 3% duplicates
    num_dupes = int(200 * 0.03)
    for _ in range(num_dupes):
        idx = random.randint(0, len(invoice_ids) - 1)
        invoice_ids.append(invoice_ids[idx])
    random.shuffle(invoice_ids)

    for inv_id in invoice_ids[:200]:
        inv_el = SubElement(root, "invoice")
        SubElement(inv_el, "invoice_id").text = f"INV-{inv_id}"

        issue_date = rand_date(2023, 2024)
        due_date = issue_date + timedelta(days=random.choice([30, 45, 60, 90]))
        SubElement(inv_el, "issued_on").text = issue_date.strftime("%Y-%m-%d")
        SubElement(inv_el, "due_date").text = due_date.strftime("%Y-%m-%d")
        SubElement(inv_el, "currency").text = random.choice(CURRENCIES)
        SubElement(inv_el, "status").text = random.choice(STATUS_VARIANTS)

        # Supplier
        supplier_el = SubElement(inv_el, "supplier")
        sup_code = f"SUP-{random.randint(100,999)}"
        SubElement(supplier_el, "code").text = sup_code
        SubElement(supplier_el, "name").text = f"{random.choice(LAST_NAMES)} {random.choice(['Corp', 'Inc', 'Ltd', 'GmbH', 'SA'])}"
        SubElement(supplier_el, "country").text = random.choice(SUPPLIER_COUNTRIES)
        SubElement(supplier_el, "category").text = random.choice(SUPPLIER_CATEGORIES)

        # Buyer
        buyer_el = SubElement(inv_el, "buyer")
        dept = random.choice(BUYER_DEPTS)
        SubElement(buyer_el, "dept_code").text = dept[0]
        SubElement(buyer_el, "dept_name").text = dept[1]
        SubElement(buyer_el, "cost_center").text = dept[2]

        # Line items
        items_el = SubElement(inv_el, "line_items")
        num_items = random.randint(1, 5)
        subtotal = 0.0
        for li in range(num_items):
            item_el = SubElement(items_el, "item")
            SubElement(item_el, "description").text = random.choice(ITEM_DESCRIPTIONS)
            qty = random.randint(1, 100)
            unit_p = round(random.uniform(10, 1000), 2)
            line_total = round(qty * unit_p, 2)
            subtotal += line_total
            SubElement(item_el, "quantity").text = str(qty)
            SubElement(item_el, "unit_price").text = str(unit_p)
            SubElement(item_el, "line_total").text = str(line_total)

        # Totals
        subtotal = round(subtotal, 2)
        vat_rate = random.choice([0.05, 0.10, 0.20])
        vat_amount = round(subtotal * vat_rate, 2)
        total_ttc = round(subtotal + vat_amount, 2)

        # 5% incorrect total
        if random.random() < 0.05:
            total_ttc = round(total_ttc + random.uniform(-100, 100), 2)

        totals_el = SubElement(inv_el, "totals")
        SubElement(totals_el, "subtotal_ht").text = str(subtotal)
        SubElement(totals_el, "vat_amount").text = str(vat_amount)
        SubElement(totals_el, "total_ttc").text = str(total_ttc)

        # Payment
        payment_el = SubElement(inv_el, "payment")
        SubElement(payment_el, "method").text = random.choice(PAYMENT_METHODS)

        # 7% missing paid_on
        if random.random() > 0.07:
            paid_date = due_date + timedelta(days=random.randint(-10, 30))
            # Mixed date formats for paid_on
            fmt_fn = random.choice(DATE_FORMATS)
            SubElement(payment_el, "paid_on").text = fmt_fn(paid_date)
        else:
            SubElement(payment_el, "paid_on").text = ""

    xml_str = tostring(root, encoding="unicode")
    pretty = parseString(xml_str).toprettyxml(indent="  ")
    path = os.path.join(OUTPUT_DIR, "dataset3_supplier_invoices.xml")
    with open(path, "w", encoding="utf-8") as f:
        f.write(pretty)
    print(f"  ✓ dataset3_supplier_invoices.xml — 200 invoices")


# ═══════════════════════════════════════════════════════════
# DATASET 4 — E-commerce Events JSON
# ═══════════════════════════════════════════════════════════
SEGMENTS = ["new", "returning", "vip", "churned"]
COUNTRY_VARIANTS = ["FR", "France", "fra", "US", "USA", "united states",
                    "DE", "Germany", "UK", "United Kingdom", "JP", "Japan"]
DEVICES = ["mobile", "desktop", "tablet"]
OS_LIST = ["iOS", "Android", "Windows", "macOS", "Linux"]
PRODUCT_IDS = [f"PROD-{i}" for i in range(100, 200)]
PRODUCT_NAMES_EC = ["Wireless Mouse", "USB-C Hub", "Standing Desk", "Monitor Arm",
                    "Mechanical Keyboard", "Webcam HD", "Noise-Cancelling Headphones",
                    "Laptop Stand", "Desk Lamp", "Ergonomic Chair"]


def generate_dataset4():
    events = []
    sessions = {}

    # Pre-generate sessions
    for _ in range(200):
        sid = str(uuid.uuid4())[:8]
        uid = f"USER-{random.randint(1000, 9999)}" if random.random() > 0.15 else None
        sessions[sid] = {
            "uid": uid,
            "segment": random.choice(SEGMENTS),
            "country": random.choice(COUNTRY_VARIANTS),
            "device": random.choice(DEVICES),
            "os": random.choice(OS_LIST),
            "base_time": rand_date(2024, 2024),
        }

    order_ids_created = []
    event_counter = 0

    for sid, session in sessions.items():
        base_time = datetime.combine(session["base_time"], datetime.min.time()) + \
                    timedelta(hours=random.randint(8, 22), minutes=random.randint(0, 59))

        # Generate sequence of events for this session
        num_events = random.randint(1, 8)
        times = sorted([base_time + timedelta(seconds=random.randint(0, 3600))
                        for _ in range(num_events)])

        # 5% out-of-order timestamps
        if random.random() < 0.05 and len(times) > 1:
            i, j = random.sample(range(len(times)), 2)
            times[i], times[j] = times[j], times[i]

        for k, ts in enumerate(times):
            event_counter += 1
            eid = f"EVT-{event_counter:06d}"

            # Decide event type based on position in session
            if k == 0:
                event_type = "product_view"
            elif k == len(times) - 1 and random.random() < 0.3:
                event_type = "purchase"
            elif random.random() < 0.1:
                event_type = "refund"
            elif random.random() < 0.4:
                event_type = "add_to_cart"
            else:
                event_type = "product_view"

            product_id = random.choice(PRODUCT_IDS)
            product_name = random.choice(PRODUCT_NAMES_EC)
            base_price = round(random.uniform(20, 500), 2)

            if event_type == "product_view":
                payload = {
                    "product_id": product_id,
                    "product_name": product_name,
                    "category": random.choice(list(PRODUCT_CATEGORIES.keys())),
                    "price": base_price,
                    "currency": "USD",
                }
            elif event_type == "add_to_cart":
                # 8% price mismatch
                cart_price = base_price if random.random() > 0.08 else round(base_price * random.uniform(0.8, 1.2), 2)
                payload = {
                    "product_id": product_id,
                    "product_name": product_name,
                    "quantity": random.randint(1, 5),
                    "price": cart_price,
                    "currency": "USD",
                }
            elif event_type == "purchase":
                qty = random.randint(1, 3)
                total = round(base_price * qty, 2)
                order_id = f"ORD-{random.randint(50000, 59999)}"
                order_ids_created.append(order_id)
                payload = {
                    "order_id": order_id,
                    "items": [{"product_id": product_id, "quantity": qty, "price": base_price}],
                    "total": total,
                    "payment_method": random.choice(PAYMENT_METHODS),
                    "currency": "USD",
                }
            elif event_type == "refund":
                # Some reference non-existent order_ids
                if order_ids_created and random.random() > 0.3:
                    ref_order = random.choice(order_ids_created)
                else:
                    ref_order = f"ORD-{random.randint(90000, 99999)}"  # non-existent
                payload = {
                    "order_id": ref_order,
                    "refund_amount": round(random.uniform(10, 300), 2),
                    "reason": random.choice(["defective", "wrong_item", "changed_mind", "late_delivery"]),
                    "currency": "USD",
                }

            event = {
                "event_id": eid,
                "ts": ts.isoformat(),
                "session_id": sid,
                "user": {
                    "uid": session["uid"],
                    "segment": session["segment"],
                    "country": session["country"],
                    "device": session["device"],
                    "os": session["os"],
                },
                "event_type": event_type,
                "payload": payload,
            }
            events.append(event)

            if len(events) >= 800:
                break
        if len(events) >= 800:
            break

    events = events[:800]
    path = os.path.join(OUTPUT_DIR, "dataset4_ecommerce_events.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2, ensure_ascii=False)
    print(f"  ✓ dataset4_ecommerce_events.json — {len(events)} events")


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("Generating research datasets...")
    generate_dataset1()
    generate_dataset2()
    generate_dataset3()
    generate_dataset4()
    print("\nAll datasets generated successfully!")
