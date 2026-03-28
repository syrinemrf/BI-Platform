"""
Generate sample datasets for BI Platform testing.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

OUTPUT_DIR = "./sample_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===========================================
# Dataset 1: E-Commerce Sales
# ===========================================
def generate_ecommerce_sales():
    n_records = 5000

    # Date range: last 2 years
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 12, 31)
    date_range = (end_date - start_date).days

    # Dimensions
    customers = [f"CUST_{i:04d}" for i in range(1, 501)]
    products = [
        ("PROD_001", "Laptop Pro 15", "Electronics", 1299.99),
        ("PROD_002", "Wireless Mouse", "Electronics", 29.99),
        ("PROD_003", "USB-C Hub", "Electronics", 49.99),
        ("PROD_004", "Mechanical Keyboard", "Electronics", 149.99),
        ("PROD_005", "Monitor 27inch", "Electronics", 399.99),
        ("PROD_006", "Headphones BT", "Electronics", 199.99),
        ("PROD_007", "Webcam HD", "Electronics", 79.99),
        ("PROD_008", "Desk Lamp LED", "Home", 34.99),
        ("PROD_009", "Office Chair", "Furniture", 299.99),
        ("PROD_010", "Standing Desk", "Furniture", 599.99),
        ("PROD_011", "Notebook Set", "Office", 12.99),
        ("PROD_012", "Pen Pack", "Office", 8.99),
        ("PROD_013", "Coffee Mug", "Home", 14.99),
        ("PROD_014", "Water Bottle", "Home", 24.99),
        ("PROD_015", "Backpack", "Accessories", 89.99),
    ]

    regions = ["North", "South", "East", "West", "Central"]
    countries = {
        "North": ["Canada", "USA"],
        "South": ["Mexico", "Brazil"],
        "East": ["UK", "France", "Germany"],
        "West": ["Japan", "Australia"],
        "Central": ["USA", "Canada"]
    }
    channels = ["Online", "Store", "Mobile App", "Partner"]
    payment_methods = ["Credit Card", "PayPal", "Bank Transfer", "Cash"]

    data = []
    for i in range(n_records):
        order_date = start_date + timedelta(days=random.randint(0, date_range))
        product = random.choice(products)
        region = random.choice(regions)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
        unit_price = product[3]
        discount = random.choices([0, 0.05, 0.10, 0.15, 0.20], weights=[60, 20, 10, 7, 3])[0]

        data.append({
            "order_id": f"ORD_{i+1:06d}",
            "order_date": order_date.strftime("%Y-%m-%d"),
            "customer_id": random.choice(customers),
            "product_id": product[0],
            "product_name": product[1],
            "category": product[2],
            "quantity": quantity,
            "unit_price": unit_price,
            "discount": discount,
            "total_amount": round(quantity * unit_price * (1 - discount), 2),
            "region": region,
            "country": random.choice(countries[region]),
            "channel": random.choice(channels),
            "payment_method": random.choice(payment_methods),
            "shipping_cost": round(random.uniform(5, 25), 2),
            "is_returned": random.choices([False, True], weights=[95, 5])[0]
        })

    df = pd.DataFrame(data)
    df.to_csv(f"{OUTPUT_DIR}/ecommerce_sales.csv", index=False)
    print(f"Generated: ecommerce_sales.csv ({len(df)} rows)")
    return df


# ===========================================
# Dataset 2: HR Employee Data
# ===========================================
def generate_hr_data():
    n_employees = 500

    departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "Support"]
    positions = {
        "Engineering": ["Software Engineer", "Senior Engineer", "Tech Lead", "Architect", "DevOps Engineer"],
        "Sales": ["Sales Rep", "Account Manager", "Sales Director", "Business Dev"],
        "Marketing": ["Marketing Specialist", "Content Manager", "SEO Analyst", "Marketing Director"],
        "HR": ["HR Specialist", "Recruiter", "HR Manager", "Training Coordinator"],
        "Finance": ["Accountant", "Financial Analyst", "Controller", "CFO"],
        "Operations": ["Operations Manager", "Logistics Coordinator", "Supply Chain Analyst"],
        "Support": ["Support Agent", "Support Lead", "Technical Support", "Customer Success"]
    }

    locations = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Paris", "London", "Berlin"]
    education = ["High School", "Bachelor's", "Master's", "PhD"]

    data = []
    for i in range(n_employees):
        dept = random.choice(departments)
        hire_date = datetime(2015, 1, 1) + timedelta(days=random.randint(0, 3650))
        years_exp = random.randint(0, 25)

        base_salary = {
            "Engineering": 85000,
            "Sales": 65000,
            "Marketing": 60000,
            "HR": 55000,
            "Finance": 70000,
            "Operations": 55000,
            "Support": 45000
        }[dept]

        salary = base_salary + (years_exp * 2000) + random.randint(-10000, 20000)

        data.append({
            "employee_id": f"EMP_{i+1:04d}",
            "first_name": random.choice(["John", "Jane", "Mike", "Sarah", "David", "Emma", "Chris", "Lisa", "Tom", "Anna"]),
            "last_name": random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Martinez", "Wilson"]),
            "email": f"employee{i+1}@company.com",
            "department": dept,
            "position": random.choice(positions[dept]),
            "location": random.choice(locations),
            "hire_date": hire_date.strftime("%Y-%m-%d"),
            "salary": round(salary, 2),
            "bonus": round(salary * random.uniform(0, 0.15), 2),
            "years_experience": years_exp,
            "education_level": random.choices(education, weights=[10, 50, 35, 5])[0],
            "performance_rating": random.choices([1, 2, 3, 4, 5], weights=[5, 10, 40, 35, 10])[0],
            "is_manager": random.choices([False, True], weights=[85, 15])[0],
            "is_remote": random.choices([False, True], weights=[60, 40])[0]
        })

    df = pd.DataFrame(data)
    df.to_csv(f"{OUTPUT_DIR}/hr_employees.csv", index=False)
    print(f"Generated: hr_employees.csv ({len(df)} rows)")
    return df


# ===========================================
# Dataset 3: Website Analytics
# ===========================================
def generate_web_analytics():
    n_records = 10000

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)
    date_range = (end_date - start_date).days

    pages = ["/home", "/products", "/about", "/contact", "/blog", "/pricing", "/signup", "/login", "/checkout", "/cart"]
    sources = ["Google", "Facebook", "Twitter", "LinkedIn", "Direct", "Email", "Referral"]
    devices = ["Desktop", "Mobile", "Tablet"]
    browsers = ["Chrome", "Firefox", "Safari", "Edge"]
    countries = ["USA", "UK", "Canada", "Germany", "France", "Australia", "Japan", "Brazil", "India", "Mexico"]

    data = []
    for i in range(n_records):
        visit_date = start_date + timedelta(days=random.randint(0, date_range))
        hour = random.choices(range(24), weights=[1,1,1,1,1,2,3,5,7,8,8,7,6,6,7,8,9,8,6,5,4,3,2,1])[0]

        data.append({
            "session_id": f"SES_{i+1:08d}",
            "visit_date": visit_date.strftime("%Y-%m-%d"),
            "visit_hour": hour,
            "user_id": f"USER_{random.randint(1, 2000):05d}" if random.random() > 0.3 else None,
            "page": random.choice(pages),
            "source": random.choices(sources, weights=[35, 20, 10, 8, 15, 7, 5])[0],
            "device": random.choices(devices, weights=[45, 45, 10])[0],
            "browser": random.choices(browsers, weights=[60, 15, 15, 10])[0],
            "country": random.choices(countries, weights=[30, 10, 8, 8, 7, 7, 6, 8, 10, 6])[0],
            "page_views": random.randint(1, 15),
            "session_duration_sec": random.randint(10, 1800),
            "bounce": random.choices([True, False], weights=[35, 65])[0],
            "converted": random.choices([True, False], weights=[3, 97])[0],
            "revenue": round(random.uniform(0, 500), 2) if random.random() < 0.03 else 0
        })

    df = pd.DataFrame(data)
    df.to_csv(f"{OUTPUT_DIR}/web_analytics.csv", index=False)
    print(f"Generated: web_analytics.csv ({len(df)} rows)")
    return df


# ===========================================
# Dataset 4: Financial Transactions
# ===========================================
def generate_financial_data():
    n_records = 3000

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 12, 31)
    date_range = (end_date - start_date).days

    accounts = [f"ACC_{i:05d}" for i in range(1, 201)]
    transaction_types = ["Deposit", "Withdrawal", "Transfer", "Payment", "Fee", "Interest"]
    categories = ["Salary", "Utilities", "Shopping", "Food", "Transport", "Entertainment", "Healthcare", "Education", "Investment"]

    data = []
    for i in range(n_records):
        tx_date = start_date + timedelta(days=random.randint(0, date_range))
        tx_type = random.choices(transaction_types, weights=[25, 20, 15, 25, 5, 10])[0]

        if tx_type == "Deposit":
            amount = random.choice([random.uniform(100, 5000), random.uniform(2000, 10000)])
        elif tx_type == "Fee":
            amount = random.uniform(5, 50)
        elif tx_type == "Interest":
            amount = random.uniform(1, 100)
        else:
            amount = random.uniform(10, 2000)

        data.append({
            "transaction_id": f"TX_{i+1:07d}",
            "transaction_date": tx_date.strftime("%Y-%m-%d"),
            "account_id": random.choice(accounts),
            "transaction_type": tx_type,
            "category": random.choice(categories) if tx_type in ["Withdrawal", "Payment"] else None,
            "amount": round(amount, 2),
            "balance_after": round(random.uniform(1000, 50000), 2),
            "currency": random.choices(["USD", "EUR", "GBP"], weights=[70, 20, 10])[0],
            "merchant": f"Merchant_{random.randint(1, 100):03d}" if tx_type == "Payment" else None,
            "is_recurring": random.choices([False, True], weights=[80, 20])[0],
            "status": random.choices(["Completed", "Pending", "Failed"], weights=[95, 4, 1])[0]
        })

    df = pd.DataFrame(data)
    df.to_csv(f"{OUTPUT_DIR}/financial_transactions.csv", index=False)
    print(f"Generated: financial_transactions.csv ({len(df)} rows)")
    return df


# ===========================================
# Generate all datasets
# ===========================================
if __name__ == "__main__":
    print("Generating sample datasets...\n")

    generate_ecommerce_sales()
    generate_hr_data()
    generate_web_analytics()
    generate_financial_data()

    print(f"\nAll datasets saved to: {os.path.abspath(OUTPUT_DIR)}")
