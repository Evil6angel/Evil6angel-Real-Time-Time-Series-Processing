import pandas as pd

input_file = "data/bitcoin_data.csv"  # Raw data
output_file = "data/bitcoin_processed.csv"  # Processed data

# Read the raw CSV file
df = pd.read_csv(input_file)

# Check for missing or invalid timestamps
print("Rows with missing or invalid timestamps:")
print(df[df['Timestamp'].isna() | ~df['Timestamp'].apply(lambda x: str(x).replace('.', '').replace('e+', '').isdigit())])

# Drop rows with missing or invalid timestamps
df = df.dropna(subset=['Timestamp'])  # Drop rows with NaN in the Timestamp column
df = df[df['Timestamp'].apply(lambda x: str(x).replace('.', '').replace('e+', '').isdigit())]  # Drop rows with non-numeric timestamps

# Convert timestamp to nanoseconds
df['Timestamp'] = df['Timestamp'].astype(float).astype(int)  # Convert to integer (nanoseconds)

# Save the processed data to a new CSV file
df.to_csv(output_file, index=False)

print(f"Processed data saved to {output_file}")
