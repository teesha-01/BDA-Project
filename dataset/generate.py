import argparse
import random
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

# ----------------------------
# Single-chunk generation
# ----------------------------
def generate_chunk_with_supply_chain_trends(chunk_id, chunk_size, seed, start_date, end_date, regions, products,
                                            shipment_modes, fulfill_via, vendors, managers, sub_classifications, base_fraud_probability):
    """
    Generates a chunk (subset) of synthetic supply chain data with fraud trends.
    Returns a Pandas DataFrame.
    """
    random.seed(seed + chunk_id)
    np.random.seed(seed + chunk_id)
    project_codes = []
    pq_numbers = []
    po_so_numbers = []
    countries = []
    managed_by = []
    fulfill_via_col = []
    vendor_inco_terms = []
    shipment_modes_col = []
    pq_first_sent_dates = []
    po_sent_dates = []
    scheduled_delivery_dates = []
    delivered_dates = []
    delivery_recorded_dates = []
    product_groups = []
    sub_classifications_col = []
    vendors_col = []
    item_descriptions = []
    molecule_brands = []
    dosages = []
    dosage_forms = []
    unit_of_measure_per_pack = []
    line_item_quantities = []
    line_item_values = []
    pack_prices = []
    unit_prices = []
    manufacturing_sites = []
    first_line_designations = []
    weights = []
    freight_costs = []
    line_item_insurance_costs = []
    fraud_labels = []
    shipment_statuses = []
    carriers = []
    units = []
    shipping_costs = []
    timestamps = []

    delta = end_date - start_date
    for i in range(chunk_size):
        global_index = chunk_id * chunk_size + i + 1

        # Unique Identifiers
        project_codes.append(f"PC-{global_index}")
        pq_numbers.append(f"PQ-{global_index}")
        po_so_numbers.append(f"POSO-{global_index}")

        # Attributes
        countries.append(random.choice(regions))
        managed_by.append(random.choice(managers))
        fulfill_via_col.append(random.choice(fulfill_via))
        vendor_inco_terms.append(f"INCO-{random.randint(1, 5)}")
        shipment_modes_col.append(random.choice(shipment_modes))
        product_groups.append(random.choice(products))
        sub_classifications_col.append(random.choice(sub_classifications))
        vendors_col.append(random.choice(vendors))
        item_descriptions.append(f"Item-{random.randint(1, 100)}")
        shipment_statuses.append(random.choice(["In Transit", "Delivered", "Pending", "Delayed"]))
        carriers.append(random.choice(["DHL", "FedEx", "UPS", "BlueDart", "USPS"]))
        units.append(random.randint(1, 500))
        shipping_costs.append(round(random.uniform(50.0, 2000.0), 2))
        timestamps.append(start_date + timedelta(seconds=random.randint(0, int(delta.total_seconds()))))

        # Dates
        random_second = random.randint(0, int(delta.total_seconds()))
        pq_first_sent_dates.append(start_date + timedelta(seconds=random_second))
        po_sent_dates.append(start_date + timedelta(seconds=random_second + random.randint(1000, 5000)))
        scheduled_delivery_dates.append(start_date + timedelta(seconds=random_second + random.randint(10000, 20000)))
        delivered_dates.append(start_date + timedelta(seconds=random_second + random.randint(30000, 40000)))
        delivery_recorded_dates.append(start_date + timedelta(seconds=random_second + random.randint(50000, 60000)))

        # Costs and Weight
        weight = round(random.uniform(1.0, 500.0), 2)
        freight_cost = round(random.uniform(50, 2000), 2)
        weights.append(weight)
        freight_costs.append(freight_cost)

        # Fraud trends
        fraud_probability = base_fraud_probability
        if freight_cost > 1500:
            fraud_probability += 0.1
        if shipment_modes_col[-1] == "Air":
            fraud_probability += 0.05
        if countries[-1] in ["Asia", "South America"]:
            fraud_probability += 0.05
        fraud_labels.append(1 if random.random() < fraud_probability else 0)

    data = {
        "Project Code": project_codes,
        "PQ#": pq_numbers,
        "PO_SO#": po_so_numbers,
        "Country": countries,
        "Managed By": managed_by,
        "Fulfill Via": fulfill_via_col,
        "Vendor INCO Term": vendor_inco_terms,
        "Shipment Mode": shipment_modes_col,
        "PQ First Sent to Client Date": pq_first_sent_dates,
        "PO Sent to Vendor Date": po_sent_dates,
        "Scheduled Delivery Date": scheduled_delivery_dates,
        "Delivered to Client Date": delivered_dates,
        "Delivery Recorded Date": delivery_recorded_dates,
        "Product Group": product_groups,
        "Sub Classification": sub_classifications_col,
        "Vendor": vendors_col,
        "Item Description": item_descriptions,
        "Molecule Brand": molecule_brands,
        "Dosage": dosages,
        "Dosage Form": dosage_forms,
        "Unit of Measure Per Pack": unit_of_measure_per_pack,
        "Line Item Quantity": line_item_quantities,
        "Line Item Value": line_item_values,
        "Pack Price": pack_prices,
        "Unit Price": unit_prices,
        "Manufacturing Site": manufacturing_sites,
        "First Line Designation": first_line_designations,
        "Weight (Kg)": weights,
        "Freight Cost (USD)": freight_costs,
        "Line Item Insurance (USD)": line_item_insurance_costs,
        "Fraud Label": fraud_labels,
        "Shipment Status": shipment_statuses,
        "Carrier": carriers,
        "Units": units,
        "Shipping Cost": shipping_costs,
        "Timestamp": timestamps,
    }

    return pd.DataFrame(data)

def generate_supply_chain_data_with_trends(num_rows=1_000_000, seed=42, n_cores=None, base_fraud_probability=0.05):
    """
    Generate synthetic supply chain data with fraud trends using multiprocessing.
    """
    if n_cores is None:
        n_cores = cpu_count()

    regions = ["North America", "Europe", "Asia", "South America", "Africa"]
    products = ["Electronics", "Furniture", "Pharmaceuticals", "Automobiles", "Clothing"]
    shipment_modes = ["Sea", "Air", "Road"]
    fulfill_via = ["Warehouse", "Direct"]
    vendors = ["Vendor A", "Vendor B", "Vendor C", "Vendor D"]
    managers = ["Manager A", "Manager B", "Manager C"]
    sub_classifications = ["Type A", "Type B", "Type C"]

    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    chunk_size = num_rows // n_cores
    remainder = num_rows % n_cores

    tasks = []
    for i in range(n_cores):
        size = chunk_size + (1 if i < remainder else 0)
        tasks.append(
            (i, size, seed, start_date, end_date, regions, products,
             shipment_modes, fulfill_via, vendors, managers, sub_classifications, base_fraud_probability)
        )

    with Pool(processes=n_cores) as pool:
        df_chunks = pool.starmap(generate_chunk_with_supply_chain_trends, tasks)

    return pd.concat(df_chunks, ignore_index=True)

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic supply chain CSV data in parallel.")
    parser.add_argument("--rows", type=int, default=1_000_000,
                        help="Number of rows to generate.")
    parser.add_argument("--output", type=str, default="supply_chain_data.csv",
                        help="Output CSV file name.")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility.")
    parser.add_argument("--cores", type=int, default=None,
                        help="Number of CPU cores to use (default: all).")
    parser.add_argument("--fraud_probability", type=float, default=0.05,
                        help="Probability of a transaction being fraud (0.0 - 1.0).")

    args = parser.parse_args()

    print(f"Generating {args.rows} rows with multiprocessing...")
    print(f"Using {args.cores if args.cores else cpu_count()} core(s).")

    start_time = time.time()
    df = generate_supply_chain_data_with_trends(
        num_rows=args.rows,
        seed=args.seed,
        n_cores=args.cores,
        base_fraud_probability=args.fraud_probability
    )

    # Save to CSV
    df.to_csv(args.output, index=False)
    elapsed = time.time() - start_time

    print(f"Data generation complete. Saved to {args.output}")
    print(f"Time taken: {elapsed:.2f} seconds")

if _name_ == "_main_":
    main()
