
import boto3
import json
import requests
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# Databricks DBU rates by plan and compute type
DBU_PRICING_BY_PLAN = {
    "Standard": {
        "Jobs Compute": 0.40,
        "All-Purpose Compute": 0.55,
        "SQL Compute": 0.22,
        "Photon Compute": 0.30
    },
    "Premium": {
        "Jobs Compute": 0.50,
        "All-Purpose Compute": 0.65,
        "SQL Compute": 0.30,
        "Photon Compute": 0.40
    },
    "Enterprise": {
        "Jobs Compute": 0.55,
        "All-Purpose Compute": 0.75,
        "SQL Compute": 0.35,
        "Photon Compute": 0.40
    }
}

GCP_INSTANCE_TYPES = {"n2-standard-2": 0.109, "n1-standard-4": 0.15}
GCP_REGIONS = ["us-central1", "us-east4"]

AWS_INSTANCE_TYPES = {"m5.xlarge": 0.192, "r5.large": 0.126, "t3.medium": 0.0416}
AWS_SPOT_DISCOUNT = 0.3  # 30% discount for spot approximation
AWS_REGIONS = ["us-east-1", "us-west-2"]

def get_aws_instance_price(instance_type, region="us-east-1", spot=False):
    try:
        if spot:
            return round(AWS_INSTANCE_TYPES.get(instance_type, 0.1) * (1 - AWS_SPOT_DISCOUNT), 4)
        client = boto3.client("pricing", region_name="us-east-1")
        response = client.get_products(
            ServiceCode='AmazonEC2',
            Filters=[
                {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                {"Type": "TERM_MATCH", "Field": "location", "Value": "US East (N. Virginia)"},
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
            ],
            MaxResults=1
        )
        product = json.loads(response['PriceList'][0])
        on_demand = product['terms']['OnDemand']
        price_dimensions = list(on_demand.values())[0]['priceDimensions']
        return float(list(price_dimensions.values())[0]['pricePerUnit']['USD'])
    except Exception:
        return None

def get_gcp_instance_price(instance_type):
    return GCP_INSTANCE_TYPES.get(instance_type)

def fetch_instance_price(cloud_provider, instance_type, region, spot):
    if cloud_provider == "AWS":
        return get_aws_instance_price(instance_type, region, spot)
    elif cloud_provider == "GCP":
        return get_gcp_instance_price(instance_type)
    return None

def calculate_databricks_cost(instance_price, num_nodes, runtime_hours, dbu_per_hour, dbu_cost_per_dbu, runs_per_day, days_per_month):
    instance_cost = instance_price * num_nodes * runtime_hours * runs_per_day * days_per_month
    dbu_cost = dbu_per_hour * num_nodes * dbu_cost_per_dbu * runtime_hours * runs_per_day * days_per_month
    return round(instance_cost + dbu_cost, 2)

def plot_monthly_costs(cost):
    months = list(range(1, 13))
    costs = [cost * m for m in months]
    fig, ax = plt.subplots()
    ax.plot(months, costs, marker='o')
    ax.set_title("Projected Annual Databricks Cost")
    ax.set_xlabel("Month")
    ax.set_ylabel("Cumulative Cost ($)")
    ax.grid(True)
    st.pyplot(fig)

def main():
    st.title("Databricks Cost Estimator (AWS/GCP with Spot & Charts)")

    cloud_provider = st.selectbox("Select Cloud Provider", ["AWS", "GCP"])
    spot = False
    if cloud_provider == "AWS":
        instance_type = st.selectbox("Instance Type", list(AWS_INSTANCE_TYPES.keys()))
        region = st.selectbox("Region", AWS_REGIONS)
        spot = st.checkbox("Use Spot Instances (Estimated 30% Discount)")
    else:
        instance_type = st.selectbox("Instance Type", list(GCP_INSTANCE_TYPES.keys()))
        region = st.selectbox("Region", GCP_REGIONS)

    plan = st.selectbox("Databricks Plan", list(DBU_PRICING_BY_PLAN.keys()))
    compute_type = st.selectbox("Compute Type", list(DBU_PRICING_BY_PLAN[plan].keys()))
    dbu_cost = DBU_PRICING_BY_PLAN[plan][compute_type]

    num_nodes = st.number_input("Number of Nodes", min_value=1, value=3)
    runtime_hours = st.number_input("Runtime per Job (hours)", min_value=0.1, value=2.5)
    dbu_per_hour = st.number_input("DBU Usage per Node per Hour", min_value=0.1, value=2.75)
    runs_per_day = st.number_input("Runs per Day", min_value=1, value=1)
    days_per_month = st.number_input("Active Days per Month", min_value=1, value=1)

    if st.button("Estimate Monthly Cost"):
        instance_price = fetch_instance_price(cloud_provider, instance_type, region, spot)
        if instance_price:
            total_cost = calculate_databricks_cost(instance_price, num_nodes, runtime_hours, dbu_per_hour, dbu_cost, runs_per_day, days_per_month)
            st.success(f"Estimated Monthly Databricks Cost: ${total_cost:.2f}")

            breakdown = {
                "Cloud": cloud_provider,
                "Plan": plan,
                "Compute Type": compute_type,
                "Spot Instance": "Yes" if spot else "No",
                "DBU Rate": f"${dbu_cost}/DBU",
                "Instance Cost/hr": f"${instance_price:.4f}",
                "Monthly Job Runs": runs_per_day * days_per_month,
                "Total Monthly Cost": f"${total_cost:.2f}"
            }
            df = pd.DataFrame([breakdown])
            st.dataframe(df)
            st.download_button("Download Breakdown CSV", df.to_csv(index=False).encode("utf-8"), "databricks_monthly_cost.csv", "text/csv")

            plot_monthly_costs(total_cost)
        else:
            st.error("Could not fetch instance pricing.")

if __name__ == "__main__":
    main()
