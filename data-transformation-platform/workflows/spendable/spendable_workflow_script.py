import pandas as pd
import numpy as np

def process_excel_workflow(raw_data_file, location_mapping_file, valid_from_date=None, valid_to_date=None, remove_min_max=False, single_max_children=None, married_max_children=None):
    """
    Complete Excel workflow automation script v2 with enhanced functionality
    
    This script includes:
    1. Wide-to-long transformation with enhanced family status (Single1-5)
    2. XYNA exception handling with special identifiers
    3. Date configuration for validFromDate and validToDate
    4. Optional min/max removal for salary outlier filtering
    5. Complete column transformations and formatting
    6. Updated marital status capitalization and userNotes formatting
    7. NEW: Single5 category using Married4 data
    8. NEW: Max children adjustment functionality
    """
    
    print("Starting Enhanced Excel workflow processing v2...")
    
    # Step 1: Load and validate data
    print("\n1. Loading data files...")
    try:
        raw_data = pd.read_csv(raw_data_file)
        location_data = pd.read_csv(location_mapping_file)
        print(f"   Raw data loaded: {raw_data.shape[0]} rows, {raw_data.shape[1]} columns")
        print(f"   Location data loaded: {location_data.shape[0]} rows, {location_data.shape[1]} columns")
    except Exception as e:
        print(f"   Error loading files: {e}")
        return None, None
    
    # Step 2: Data validation
    print("\n2. Validating data structure...")
    required_cols_raw = ['Country', 'region', 'Currency', 'GrossFrom', 'GrossTo',
                        'Single', 'Married', 'Married1', 'Married2', 'Married3', 'Married4', 'Married5']
    required_cols_mapping = ['Mercer Location Concatenation', 'location_uuid', 'location_type']
    
    missing_raw = [col for col in required_cols_raw if col not in raw_data.columns]
    missing_mapping = [col for col in required_cols_mapping if col not in location_data.columns]
    
    if missing_raw:
        print(f"   Warning: Missing columns in raw data: {missing_raw}")
    if missing_mapping:
        print(f"   Warning: Missing columns in mapping data: {missing_mapping}")
        return None, None
    
    # Clean column names
    raw_data.columns = raw_data.columns.str.strip()
    location_data.columns = location_data.columns.str.strip()
    
    # Step 3: Enhanced wide-to-long transformation with Single1-5 expansion
    print("\n3. Performing enhanced wide-to-long transformation (including Single5)...")
    id_vars = ['Country', 'region', 'Currency', 'GrossFrom', 'GrossTo']
    
    # Create expanded family status mapping with Single5
    family_status_mapping = {
        'Single': 'Single',
        'Single1': 'Married',   # Single1 uses Married data
        'Single2': 'Married1',  # Single2 uses Married1 data
        'Single3': 'Married2',  # Single3 uses Married2 data
        'Single4': 'Married3',  # Single4 uses Married3 data
        'Single5': 'Married4',  # Single5 uses Married4 data (NEW)
        'Married': 'Married',
        'Married1': 'Married1',
        'Married2': 'Married2',
        'Married3': 'Married3',
        'Married4': 'Married4',
        'Married5': 'Married5'
    }
    
    # Create long format data
    long_data_list = []
    for family_status, source_col in family_status_mapping.items():
        if source_col in raw_data.columns:
            temp_df = raw_data[id_vars + [source_col]].copy()
            temp_df['family_status'] = family_status
            temp_df['cost'] = temp_df[source_col]
            temp_df = temp_df.drop(columns=[source_col])
            long_data_list.append(temp_df)
    
    long_data = pd.concat(long_data_list, ignore_index=True)
    print(f"   Enhanced transformation: {raw_data.shape[0]} rows to {long_data.shape[0]} rows")
    print(f"   Family status categories: {long_data['family_status'].nunique()}")
    
    # Step 4: Create location keys
    print("\n4. Creating location keys...")
    long_data['region_clean'] = long_data['region'].fillna('NA')
    long_data['location_key'] = long_data['Country'] + long_data['region_clean']
    print(f"   Created {long_data['location_key'].nunique()} unique location keys")
    
    # Step 5: Match with location data (with XYNA exception handling)
    print("\n5. Matching with Mercer location data (XYNA exception handling)...")
    # Identify XYNA records before merge
    long_data['is_XYNA'] = long_data['location_key'] == 'XYNA'
    
    # Perform left join
    merged_data = pd.merge(long_data,
                          location_data[['Mercer Location Concatenation', 'location_uuid', 'location_type']],
                          left_on='location_key',
                          right_on='Mercer Location Concatenation',
                          how='left')
    
    # Step 6: Enhanced missing location identification (with XYNA exception)
    print("\n6. Identifying missing locations (XYNA exception handling)...")
    # Create masks for different cases
    xyna_mask = merged_data['is_XYNA'] == True
    matched_mask = merged_data['location_uuid'].notna()
    missing_mask = merged_data['location_uuid'].isna() & ~xyna_mask
    
    # Split data using masks
    xyna_records = merged_data[xyna_mask].copy()
    successfully_matched = merged_data[matched_mask].copy()
    missing_locations = merged_data[missing_mask].copy()
    
    print(f"   Total records: {len(merged_data)}")
    print(f"   Successfully matched: {len(successfully_matched)}")
    print(f"   XYNA records: {len(xyna_records)}")
    print(f"   Missing locations: {len(missing_locations)}")
    
    # Step 7: Min/Max removal (optional, includes XYNA records)
    if remove_min_max and (len(successfully_matched) > 0 or len(xyna_records) > 0):
        print("\n7. Applying min/max removal (including XYNA records)...")
        # Combine matched and XYNA records for min/max processing
        combined_for_minmax = pd.concat([successfully_matched, xyna_records], ignore_index=True)
        
        if len(combined_for_minmax) > 0:
            # Create grouping key - treat blank/null locationUuid as 'BLANK_UUID'
            combined_for_minmax['group_key'] = combined_for_minmax['location_uuid'].fillna('BLANK_UUID')
            
            # Calculate min/max for each group with proper rounding
            combined_for_minmax['grossFrom_rounded'] = combined_for_minmax['GrossFrom'].round(2)
            combined_for_minmax['grossTo_rounded'] = combined_for_minmax['GrossTo'].round(2)
            
            # Group statistics
            group_stats = combined_for_minmax.groupby('group_key').agg({
                'grossFrom_rounded': ['min', 'max', 'count'],
                'grossTo_rounded': ['min', 'max', 'count']
            }).round(2)
            
            # Flatten column names
            group_stats.columns = ['_'.join(col).strip() for col in group_stats.columns.values]
            group_stats = group_stats.reset_index()
            
            # Merge back to main data
            combined_for_minmax = pd.merge(combined_for_minmax, group_stats, on='group_key', how='left')
            
            # Apply min/max removal logic
            grossFrom_removed = 0
            grossTo_removed = 0
            
            # Remove minimum grossFrom values (only if group has more than 1 record)
            min_mask = ((combined_for_minmax['grossFrom_rounded'] == combined_for_minmax['grossFrom_rounded_min']) &
                       (combined_for_minmax['grossFrom_rounded_count'] > 1))
            combined_for_minmax.loc[min_mask, 'GrossFrom'] = np.nan
            grossFrom_removed = min_mask.sum()
            
            # Remove maximum grossTo values (only if group has more than 1 record)
            max_mask = ((combined_for_minmax['grossTo_rounded'] == combined_for_minmax['grossTo_rounded_max']) &
                       (combined_for_minmax['grossTo_rounded_count'] > 1))
            combined_for_minmax.loc[max_mask, 'GrossTo'] = np.nan
            grossTo_removed = max_mask.sum()
            
            print(f"   Removed {grossFrom_removed} minimum grossFrom values")
            print(f"   Removed {grossTo_removed} maximum grossTo values")
            
            # Clean up temporary columns
            combined_for_minmax = combined_for_minmax.drop(columns=[col for col in combined_for_minmax.columns
                                                                   if any(temp in col for temp in ['rounded', '_min', '_max', '_count', 'group_key'])])
            
            # Split back into matched and XYNA
            successfully_matched = combined_for_minmax[combined_for_minmax['is_XYNA'] == False].copy()
            xyna_records = combined_for_minmax[combined_for_minmax['is_XYNA'] == True].copy()
    else:
        print("\n7. Skipping min/max removal (not requested or insufficient data)")
    
    # Step 8: Prepare final datasets with enhanced transformations
    print("\n8. Preparing enhanced output datasets...")
    # Combine successfully matched and XYNA records for final output
    final_base = pd.concat([successfully_matched, xyna_records], ignore_index=True)
    
    if len(final_base) > 0:
        # Create enhanced columns
        final_output = final_base.copy()
        
        # Create maritalStatus column (remove numbers and capitalize to UPPERCASE)
        final_output['maritalStatus'] = final_output['family_status'].str.replace(r'\d+$', '', regex=True).str.upper()
        
        # Create numberOfChildren column (extract numbers from end, use "0" for no match)
        def extract_children_count(family_status):
            import re
            match = re.search(r'(\d+)$', str(family_status))
            return str(match.group()) if match else "0"
        
        final_output['numberOfChildren'] = final_output['family_status'].apply(extract_children_count)
        
        # Handle XYNA records - set blank locationUuid and WORLD locationType
        final_output.loc[final_output['is_XYNA'] == True, 'location_uuid'] = ''
        final_output.loc[final_output['is_XYNA'] == True, 'location_type'] = 'WORLD'
        
        # Create serviceType column ("GEC_POLICY" for XYNA, blank for others)
        final_output['serviceType'] = ''
        final_output.loc[final_output['is_XYNA'] == True, 'serviceType'] = 'GEC_POLICY'
        
        # Create userNotes column with XYNA exception for GEC and hyphenation
        def format_user_notes(location_key):
            if location_key == 'XYNA':
                return 'MercerLocation: GEC'
            elif len(str(location_key)) >= 2:
                formatted_key = str(location_key)[:2] + '-' + str(location_key)[2:]
                return f'MercerLocation: {formatted_key}'
            else:
                return f'MercerLocation: {location_key}'
        
        final_output['userNotes'] = final_output['location_key'].apply(format_user_notes)
        
        # Add date columns if provided
        if valid_from_date:
            final_output['validFromDate'] = valid_from_date
        else:
            final_output['validFromDate'] = ''
        
        if valid_to_date:
            final_output['validToDate'] = valid_to_date
        else:
            final_output['validToDate'] = ''
        
        # Step 8.5: NEW - Max Children Adjustment Logic
        if single_max_children or married_max_children:
            print("\n8.5. Applying max children adjustments...")
            
            # Convert inputs to string for comparison
            if single_max_children:
                single_max_str = str(single_max_children).strip()
                if single_max_str:
                    # Blank out numberOfChildren for SINGLE rows with the specified number
                    single_mask = (final_output['maritalStatus'] == 'SINGLE') & (final_output['numberOfChildren'] == single_max_str)
                    records_affected_single = single_mask.sum()
                    final_output.loc[single_mask, 'numberOfChildren'] = ''
                    print(f"   Set {records_affected_single} SINGLE records with {single_max_str} children to unlimited (blank)")
            
            if married_max_children:
                married_max_str = str(married_max_children).strip()
                if married_max_str:
                    # Blank out numberOfChildren for MARRIED rows with the specified number
                    married_mask = (final_output['maritalStatus'] == 'MARRIED') & (final_output['numberOfChildren'] == married_max_str)
                    records_affected_married = married_mask.sum()
                    final_output.loc[married_mask, 'numberOfChildren'] = ''
                    print(f"   Set {records_affected_married} MARRIED records with {married_max_str} children to unlimited (blank)")
        
        # Rename columns
        final_output = final_output.rename(columns={
            'GrossFrom': 'grossFrom',
            'GrossTo': 'grossTo',
            'Currency': 'currency',
            'location_uuid': 'locationUuid',
            'location_type': 'locationType'
        })
        
        # Select and reorder final columns
        final_columns = ['locationUuid', 'locationType', 'grossFrom', 'grossTo', 'maritalStatus',
                        'numberOfChildren', 'cost', 'currency', 'serviceType', 'validFromDate',
                        'validToDate', 'userNotes']
        final_output = final_output[final_columns].copy()
        final_output = final_output.sort_values(['locationUuid', 'maritalStatus', 'numberOfChildren'], na_position='first')
    
    else:
        # Create empty DataFrame with correct columns
        final_columns = ['locationUuid', 'locationType', 'grossFrom', 'grossTo', 'maritalStatus',
                        'numberOfChildren', 'cost', 'currency', 'serviceType', 'validFromDate',
                        'validToDate', 'userNotes']
        final_output = pd.DataFrame(columns=final_columns)
    
    # Missing locations dataset (unchanged)
    if len(missing_locations) > 0:
        missing_output = missing_locations[['Country', 'region', 'location_key', 'family_status', 'cost']].copy()
        missing_output = missing_output.sort_values(['Country', 'region', 'family_status'])
    else:
        missing_output = pd.DataFrame(columns=['Country', 'region', 'location_key', 'family_status', 'cost'])
    
    # Step 9: Summary statistics
    print("\n9. Enhanced Summary Statistics v2:")
    print(f"   Original records: {len(raw_data)}")
    print(f"   Transformed records: {len(long_data)}")
    print(f"   Successfully matched: {len(successfully_matched)}")
    print(f"   XYNA records: {len(xyna_records)}")
    print(f"   Final output records: {len(final_output)}")
    print(f"   Missing locations: {len(missing_locations)}")
    
    if len(long_data) > 0:
        match_rate = (len(successfully_matched) + len(xyna_records)) / len(long_data) * 100
        print(f"   Overall match rate: {match_rate:.2f}%")
    
    if len(final_output) > 0:
        print(f"   Unique locations in final dataset: {final_output['locationUuid'].nunique()}")
        print(f"   Marital status categories: {final_output['maritalStatus'].nunique()}")
        print(f"   Family categories processed: {len(family_status_mapping)} (including Single5)")
        
        # Show distribution of numberOfChildren including blanks for unlimited
        children_dist = final_output['numberOfChildren'].value_counts(dropna=False)
        print(f"   Number of children distribution:")
        for value, count in children_dist.head(10).items():
            display_value = "Unlimited" if pd.isna(value) or value == '' else value
            print(f"     {display_value}: {count} records")
    
    print("\nProcessing v2 completed successfully!")
    return missing_output, final_output

# Execute the workflow when run directly
if __name__ == "__main__":
    # Example usage
    missing_data, final_data = process_excel_workflow(
        'raw_data.csv',
        'location_mapping.csv',
        valid_from_date='2025-07-01T00:00:00',
        valid_to_date='2025-12-31T00:00:00',
        remove_min_max=True,
        single_max_children=4,
        married_max_children=5
    )