import csv
import requests
import os

# Define the path to your CSV file
csv_file_path = input("enter path to CSV").strip()
download_directory = input("enter path to directory").strip()

# Create the download directory if it doesn't exist
os.makedirs(download_directory, exist_ok=True)

# Read the CSV file
with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
    csvreader = csv.DictReader(csvfile)

    # Iterate through each row in the CSV file
    for row in csvreader:
        file_url = row['File URL']
        file_name = row['File Name']

        # Define the path to save the downloaded file
        file_path = os.path.join(download_directory, file_name)

        # Download the file and save it to the specified path
        response = requests.get(file_url)

        if response.status_code == 200:
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f"Downloaded and saved: {file_name}")
        else:
            print(f"Failed to download: {file_url}")

print("All files downloaded.")


# https://dl.library.ucla.edu/solr/select?q=PID:edu.ucla.library.specialCollections.losAngelesAqueduct*&fl=mods_identifier_local_ss&fl=PID&wt=csv&rows=5000
#
# https://dl.library.ucla.edu/solr/select?q=PID:edu.ucla.library.specialCollections.losAngelesAqueduct*&wt=xml&fl=PID,mods*,bs*,dc*,fgs_label_s,PID
