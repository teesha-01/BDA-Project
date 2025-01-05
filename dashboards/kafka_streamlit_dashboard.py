import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.express as px

# Kafka Configuration
KAFKA_TOPIC = "supply_chain_data"
KAFKA_SERVER = "kafka:9092"

# Streamlit Dashboard
st.title("Kafka Real-Time Supply Chain Dashboard")

# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="streamlit-dashboard",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# Collect Data
data = []
for message in consumer:
    value = message.value
    data.append(value)
    if len(data) >= 100:  # Limit the number of messages for real-time visualization
        break

# Convert Data to DataFrame
df = pd.DataFrame(data)

# Display Data
if not df.empty:
    st.markdown("### Supply Chain Data")
    st.dataframe(df)

    # Visualization: Fraud Analysis by Product Group
    if "cf1:Product Group" in df.columns and "cf1:Fraud Label" in df.columns:
        fraud_by_product = (
            df.groupby("cf1:Product Group")["cf1:Fraud Label"]
            .value_counts(normalize=True)
            .unstack()
            .fillna(0) * 100
        )
        fraud_by_product_plot = fraud_by_product.plot(
            kind="bar", stacked=True, figsize=(10, 6), title="Fraud Analysis by Product Group (%)"
        )
        st.pyplot(fraud_by_product_plot.figure)

    # Visualization: Freight Cost Distribution
    if "cf1:Freight Cost (USD)" in df.columns:
        fig_freight = px.histogram(
            df,
            x="cf1:Freight Cost (USD)",
            nbins=20,
            title="Distribution of Freight Costs (USD)",
        )
        st.plotly_chart(fig_freight)

    # Visualization: Transactions by Shipment Mode
    if "cf1:Shipment Mode" in df.columns:
        shipment_mode_counts = df["cf1:Shipment Mode"].value_counts().reset_index()
        shipment_mode_counts.columns = ["Shipment Mode", "Count"]
        fig_shipment = px.bar(
            shipment_mode_counts,
            x="Shipment Mode",
            y="Count",
            title="Transactions by Shipment Mode",
        )
        st.plotly_chart(fig_shipment)

    # Visualization: Fraud by Country
    if "cf1:Country" in df.columns and "cf1:Fraud Label" in df.columns:
        fraud_by_country = (
            df.groupby("cf1:Country")["cf1:Fraud Label"]
            .value_counts(normalize=True)
            .unstack()
            .fillna(0) * 100
        )
        fraud_by_country_plot = fraud_by_country.plot(
            kind="bar", stacked=True, figsize=(10, 6), title="Fraud Analysis by Country (%)"
        )
        st.pyplot(fraud_by_country_plot.figure)

    # Visualization: Weight vs Freight Cost
    if "cf1:Weight_Kilograms" in df.columns and "cf1:Freight Cost (USD)" in df.columns:
        fig_weight_freight = px.scatter(
            df,
            x="cf1:Weight_Kilograms",
            y="cf1:Freight Cost (USD)",
            color="cf1:Fraud Label",
            title="Weight vs Freight Cost",
        )
        st.plotly_chart(fig_weight_freight)

    # Visualization: Freight Cost by Project Code
    if "cf1:Project Code" in df.columns and "cf1:Freight Cost (USD)" in df.columns:
        freight_cost_by_project = df.groupby("cf1:Project Code")["cf1:Freight Cost (USD)"].sum().reset_index()
        fig_project_freight = px.bar(
            freight_cost_by_project,
            x="cf1:Project Code",
            y="cf1:Freight Cost (USD)",
            title="Freight Cost by Project Code",
        )
        st.plotly_chart(fig_project_freight)

    # Visualization: Line Item Quantity by Product Group
    if "cf1:Product Group" in df.columns and "cf1:Line Item Quantity" in df.columns:
        quantity_by_product_group = df.groupby("cf1:Product Group")["cf1:Line Item Quantity"].sum().reset_index()
        fig_quantity_product = px.bar(
            quantity_by_product_group,
            x="cf1:Product Group",
            y="cf1:Line Item Quantity",
            title="Line Item Quantity by Product Group",
        )
        st.plotly_chart(fig_quantity_product)
else:
    st.markdown("No data available.")
