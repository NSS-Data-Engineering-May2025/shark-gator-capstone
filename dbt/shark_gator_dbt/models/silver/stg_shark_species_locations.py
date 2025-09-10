from datetime import datetime, timezone

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['pandas'],
        tags= ['staging', 'silver', 'attacks', 'sharks', 'monthly'],
    )
    area_mapping = {
        # Australia
        'New South ales': 'New South Wales',
        'Westerm Australia': 'Western Australia',
        # South Africa
        'Western Province': 'Western Cape Province',
        'Easten Cape Province': 'Eastern Cape Province',
        'Eastern Province': 'Eastern Cape Province',
        # United States
        'Noirth Carolina': 'North Carolina',
        'St. Marys Parish': "St. Mary's Parish",
        # New Zealand
        'South Island?': 'South Island',
        # Papua New Guinea
        'Near Bougainville (North Solomons)': 'Bougainville (North Solomons)',
        'New Ireland, Bismarck Archipelago': 'New Ireland Province, Bismarck Archipelago',
        'Northern (Oro) Province': 'Northern Province',
        # Mexico
        'Guerrrero': 'Guerrero',
        'Guerro': 'Guerrero',
        'Quinta Roo': 'Quintana Roo',
        'Baja California Sur': 'Baja California Sur',
        'Baja California': 'Baja California', # Keeping distinct from Baja California Sur
        # Vanuatu
        'Malampa Province': 'Malampa Province', 
        'Sandaun Province': 'Sandaun Province', # Papua New Guinea province, kept separate from Sanma
        'Sanma Province': 'Sanma Province',
        # Solomon Islands
        'Malaita Province': 'Malaita Province', 
        'Makora-Ulawa Province': 'Makira-Ulawa Province',
        'Makira-Uluwa Province': 'Makira-Ulawa Province',
        # Fiji
        'Tavenui': 'Taveuni',
        'Vita Levu': 'Viti Levu',
        'Vitu Levu': 'Viti Levu',
        'Lomaiviti Provine': 'Lomaiviti Province',
        # Italy
        'Tyrrenian Sea': 'Tyrrhenian Sea',
        'Liguaria': 'Liguria',
        # Iraq
        'Shatt-el-Arab River': 'Shatt-al-Arab River',
        'Shat-Al-Arab River': 'Shatt-al-Arab River',
        # Mozambique
        'Inhambe Province': 'Inhambane Province',
        'Bay of Maputu': 'Bay of Maputo',
        # France (Reunion Island)
        '5aint-Denis': 'Saint-Denis',
        "L'Etang-Salé": "L'Etang-Sale",
        'Saint-Benoît': 'Saint-Benoit',
        # Croatia
        'Split-Dalmatia Count,': 'Split-Dalmatia County',
        # Costa Rica
        'Guanacoste': 'Guanacaste',
        # Panama
        'Colón Province': 'Colon Province',
        # Cuba
        'Holguín Province': 'Holguin Province',
        # Chile
        'Elqui Province': 'Elqui Province',
        # Greece
        'Island of Volos': 'Volos',
        'Island of Kos': 'Kos',
        # Virgin Islands
        'Virgin Islands': 'US Virgin Islands',
        # Offshore Locations
        'Off coast of West Africa': 'Off the coast of West Africa',
        'Off South American coast': 'Off the coast of South America',
        'Cape Haitien': 'Off Cape Haitien',
        # Belize
        'Ambergris Cay': 'Ambergris Caye',
        # South Korea
        "South Ch'ungch'ong Province": 'South Chungcheong Province',
        # New Entries from Fuzzy Match Analysis
        'Tuamotos': 'Tuamotus',
        'Rombion Province': 'Romblon Province',
        'Galica': 'Galicia',
        'Cook islans': 'Cook Islands',
        # Locations to keep separate (Fuzzy match was incorrect)
        'Southern Province': 'South Province',
        'South Province': 'South Province',
        'Samaná Province': 'Samana Province',
        'Northern Province': 'North Province',
        'Baja California': 'Baja California' # A separate state from Baja California Sur
    }

    shark_species_locations = dbt.source("bronze_source","RAW_SHARK_SPECIES_LOCATIONS").to_pandas()

    # Apply regex to extract shark species
    shark_species_locations['SPECIES'] = shark_species_locations['SPECIES'].str.lower().str.extract(r'([A-Za-z\- ]+shark)')[0]
    shark_species_locations['SPECIES'] = shark_species_locations['SPECIES'].str.strip()
    # Drop rows where species couldn't be identified
    shark_species_locations = shark_species_locations.dropna(subset=['SPECIES'])

    shark_species_locations['LOCATION'] = shark_species_locations['LOCATION'].str.replace(r'\s+', ' ', regex=True)
    shark_species_locations['LOCATION'] = shark_species_locations['LOCATION'].str.strip()        # Remove leading/trailing whitespace
    shark_species_locations['LOCATION'] = shark_species_locations['LOCATION'].str.strip('"')    # Remove surrounding quotes
    shark_species_locations['LOCATION'] = shark_species_locations['LOCATION'].str.title()

    shark_species_locations['AREA'] = shark_species_locations['AREA'].str.replace(r'\s+', ' ', regex=True)    
    # Apply mapping to create a cleaned column
    shark_species_locations['AREA'] = shark_species_locations['AREA'].replace(area_mapping)

    shark_species_locations['ATTACK_ID']= shark_species_locations['ORIGINAL_ORDER']

    shark_species_locations['CLEANED_TIMESTAMP_UTC']= datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    return session.create_dataframe(shark_species_locations[["ATTACK_ID", "AREA", "LOCATION", "SPECIES", "CLEANED_TIMESTAMP_UTC"]])