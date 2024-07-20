import requests
import pandas as pd

# Solr query parameters
solr_url = 'https://dl.library.ucla.edu/solr/select'
params = {
    'q': 'PID:edu.ucla.library.specialCollections.losAngelesAqueduct*',
    'fl':'PID,fgs_label_s,mods_identifier_local_ss,mods_titleInfo_title_hlt,mods_language_languageTerm_text_ms,mods_language_languageTerm_code_s,mods_originInfo_publisher_ms,mods_note_s,dc.contributor,mods_name_personal_authority_lcnaf_namePart_ms,mods_physicalDescription_extent_s,mods_typeOfResource_s,mods_accessCondition_copyrightstatus_s,mods_relatedItem_location_url_ss,dc.relation,mods_subject_topic_ms,mods_accessCondition_publicationstatus_hlt,mods_accessCondition_use_and_reproduction_hlt,mods_genre_authority_aat_hlt,mods_identifier_displayLabel_mt,mods_identifier_hlt,dc.description,mods_originInfo_dateCreated_hlt,mods_originInfo_encoding_iso8601_dateCreated_hlt,mods_relatedItem_titleInfo_title_hlt,mods_subject_authority_lcnaf_name_namePart_hlt,mods_subject_cartographics_coordinates_mlt,mods_subject_geographic_ms,mods_subject_name_namePart_ms',
    'wt': 'csv',
    'rows': 10000
}

# Function to download and clean Solr CSV data
def download_and_clean_csv(solr_url, params, output_file):
    try:
        # Download CSV data from Solr
        response = requests.get(solr_url, params=params)
        with open(output_file, 'wb') as f:
            f.write(response.content)

        # Read CSV into a DataFrame
        df = pd.read_csv(output_file)
        df.rename(columns = {'fgs_label_s':'File Name', 'mods_identifier_local_ss':'Local identifier', 'mods_titleInfo_title_hlt':'Title', 'mods_language_languageTerm_text_ms': 'Language Name', 'mods_language_languageTerm_code_s':'Language', 'mods_originInfo_publisher_ms':'Publisher.publisherName', 'mods_name_personal_authority_lcnaf_namePart_ms':'Name.creator', 'dc.contributor':'Name.contributor', 'mods_note_s':'Description.note', 'mods_physicalDescription_extent_s':'Format.extent', 'mods_typeOfResource_s':'Type.typeOfResource', 'mods_accessCondition_copyrightstatus_s':'Rights.copyrightStatus', 'mods_accessCondition_publicationstatus_hlt':'Rights.publishedStatus', 'mods_relatedItem_location_url_ss':'Finding Aid URL', 'dc.relation':'Related Items', 'mods_subject_topic_ms':'Subject topic', 'mods_accessCondition_use_and_reproduction_hlt':'Rights.statementLocal', 'mods_genre_authority_aat_hlt':'Genre', 'mods_identifier_displayLabel_mt':'Collection label', 'mods_identifier_hlt':'Collection number', 'mods_originInfo_dateCreated_hlt':'Date.created', 'mods_originInfo_encoding_iso8601_dateCreated_hlt':'Date.normalized', 'mods_relatedItem_titleInfo_title_s':'Archival Collection Title', 'mods_subject_cartographics_coordinates_mlt':'Coordinates', 'mods_subject_geographic_ms':'Subject geographic', 'dc.description':'References', 'mods_subject_name_namePart_ms':'Subject name'},
                             inplace = True)

        # Remove empty columns
        #df_cleaned = df.dropna(axis=1, how='all')

        # split out coords
        df['Coordinates'].dropna(inplace = True)
        new = df["Coordinates"].str.split(",", n = 1, expand = True)
        df["Description.latitude"]= new[0]
        df["Description.longitude"]= new[1]
        df.drop(columns =["Coordinates"], inplace = True)

        # Save cleaned DataFrame back to CSV
        #cleaned_output_file = output_file.replace('.csv', '-cleaned.csv')
        #df_cleaned = df_cleaned[sorted(df_cleaned.columns)]
        df = df.replace(r'\\', '', regex=True)


        # Create multiple new empty columns
        new_columns = ['Object Type', 'Parent ARK', 'Item ARK', 'Item Sequence']
        for col in new_columns:
            df[col] = ''
        df['Object Type'] = ('Work')
        # List of columns to move to the front
        #columns_to_move = ['Object Type', 'Parent ARK', 'Item ARK', 'Item Sequence']
        remaining_columns = [col for col in df.columns if col not in new_columns]
        df = df[new_columns + remaining_columns]


        df.to_csv(output_file, index = False, encoding = 'utf-8')


        print(f"Cleaned CSV saved to '{output_file}'")

    except requests.RequestException as e:
        print(f"Error fetching data from Solr: {e}")

# Call the function to download and clean Solr CSV data
output_csv = 'output-metaedatat.csv'
download_and_clean_csv(solr_url, params, output_csv)
