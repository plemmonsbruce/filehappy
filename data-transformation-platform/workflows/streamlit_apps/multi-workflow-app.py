# Multi-Workflow Data Transformation Platform
# Enhanced Streamlit Application Framework

import streamlit as st
import pandas as pd
from datetime import date
from workflows.spendable.excel_workflow_script import process_excel_workflow

# Import placeholder modules for future workflows (you'll create these)
# from cola_workflow import process_cola_workflow
# from housing_workflow import process_housing_workflow  
# from per_diem_workflow import process_per_diem_workflow

st.set_page_config(
    page_title="Data Transformation Platform", 
    page_icon="🔄", 
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    """Main application function with workflow selection"""
    
    # Header
    st.title("🔄 Data Transformation Platform")
    st.markdown("---")
    
    # Workflow Selection Section
    st.header("📋 Select Transformation Type")
    
    # Create columns for workflow selection
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        **Choose the type of data transformation you'd like to perform:**
        - **Spendable**: Transform cost of living allowance data with family status expansion
        - **COLA**: Cost of Living Allowance calculations (Coming Soon)
        - **Housing**: Housing allowance transformations (Coming Soon) 
        - **Per Diem**: Daily allowance processing (Coming Soon)
        """)
    
    with col2:
        st.info("💡 **Tip**: Each workflow type has its own specialized processing logic and configuration options.")
    
    # Workflow selector
    workflow_type = st.selectbox(
        "**Select Workflow Type**",
        options=["Spendable", "COLA", "Housing", "Per Diem"],
        index=0,
        key="workflow_selection",
        help="Choose the type of data transformation workflow you want to use"
    )
    
    st.markdown("---")
    
    # Route to appropriate workflow based on selection
    if workflow_type == "Spendable":
        render_spendable_workflow()
    elif workflow_type == "COLA":
        render_cola_workflow()
    elif workflow_type == "Housing":
        render_housing_workflow()
    elif workflow_type == "Per Diem":
        render_per_diem_workflow()

def render_spendable_workflow():
    """Render the existing Spendable workflow interface"""
    
    st.header("💰 Spendable Data Transformation")
    st.markdown("Transform your Excel data with comprehensive family status processing and enhanced formatting.")
    
    # Create layout columns
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📁 File Upload")
        raw_data_file = st.file_uploader(
            "Upload Raw Data CSV", 
            type=["csv"],
            help="Upload your main data file with family status columns",
            key="spendable_raw_data"
        )
        
        location_mapping_file = st.file_uploader(
            "Upload Location Mapping CSV", 
            type=["csv"],
            help="Upload the Mercer location mapping file",
            key="spendable_location_mapping"
        )
    
    with col2:
        st.subheader("⚙️ Processing Options")
        
        # Date configuration
        st.markdown("**📅 Date Configuration**")
        
        use_from_date = st.checkbox("Set Valid From Date", key="spendable_use_from_date")
        valid_from_date = None
        if use_from_date:
            from_date = st.date_input("Valid From Date", value=date.today(), key="spendable_from_date")
            valid_from_date = f"{from_date}T00:00:00"
            st.success(f"Valid From Date: {valid_from_date}")
        
        use_to_date = st.checkbox("Set Valid To Date", key="spendable_use_to_date") 
        valid_to_date = None
        if use_to_date:
            to_date = st.date_input("Valid To Date", value=date.today(), key="spendable_to_date")
            valid_to_date = f"{to_date}T00:00:00"
            st.success(f"Valid To Date: {valid_to_date}")
        
        # Statistical options
        st.markdown("**📊 Statistical Options**")
        remove_min_max = st.checkbox(
            "Remove Min and Max Values",
            help="Remove minimum grossFrom and maximum grossTo values for each location to eliminate outliers",
            key="spendable_remove_min_max"
        )
        
        # Max children adjustment
        st.markdown("**👶 Max Number of Children Adjustments**")
        single_max_children_input = st.text_input(
            "Single Max Children Column", 
            help="Enter the max number of children for SINGLE (e.g., '4')",
            key="spendable_single_max"
        )
        married_max_children_input = st.text_input(
            "Married Max Children Column", 
            help="Enter the max number of children for MARRIED (e.g., '5')",
            key="spendable_married_max"
        )
        
        # Convert to int or None
        single_max_children = int(single_max_children_input) if single_max_children_input.isdigit() else None
        married_max_children = int(married_max_children_input) if married_max_children_input.isdigit() else None
    
    # Processing section
    if raw_data_file and location_mapping_file:
        st.header("🔄 Process Data")
        
        if st.button("🚀 Process Spendable Data", type="primary", key="spendable_process"):
            with st.spinner("Processing your spendable data..."):
                try:
                    missing_output, final_output = process_excel_workflow(
                        raw_data_file, 
                        location_mapping_file,
                        valid_from_date=valid_from_date,
                        valid_to_date=valid_to_date,
                        remove_min_max=remove_min_max,
                        single_max_children=single_max_children,
                        married_max_children=married_max_children
                    )
                    
                    if missing_output is not None and final_output is not None:
                        display_spendable_results(missing_output, final_output, valid_from_date, valid_to_date, remove_min_max, single_max_children, married_max_children)
                    else:
                        st.error("❌ Processing failed. Please check your files and try again.")
                        
                except Exception as e:
                    st.error(f"❌ Error during processing: {str(e)}")
                    st.info("Please check that your files have the required columns and proper formatting.")
    else:
        st.info("👆 Please upload both CSV files to begin spendable processing")

def display_spendable_results(missing_output, final_output, valid_from_date, valid_to_date, remove_min_max, single_max_children, married_max_children):
    """Display processing results for spendable workflow"""
    
    st.success("✅ Spendable processing completed successfully!")
    
    # Results summary
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Final Records", len(final_output))
    with col2:
        st.metric("Missing Locations", len(missing_output))
    with col3:
        unique_locations = final_output['locationUuid'].nunique() if len(final_output) > 0 else 0
        st.metric("Unique Locations", unique_locations)
    with col4:
        gec_policy_count = (final_output['serviceType'] == 'GEC_POLICY').sum() if len(final_output) > 0 else 0
        st.metric("XYNA/GEC Records", gec_policy_count)
    with col5:
        unlimited_children = ((final_output['numberOfChildren'] == '') & (final_output['maritalStatus'].isin(['SINGLE', 'MARRIED']))).sum() if len(final_output) > 0 else 0
        st.metric("Unlimited Children", unlimited_children)
    
    # Data preview
    if len(final_output) > 0:
        st.subheader("📋 Data Preview")
        st.dataframe(final_output.head(10), use_container_width=True)
        
        # Show sample of formatting
        if len(final_output) > 0:
            st.subheader("✨ Enhanced Formatting Examples")
            sample_data = final_output[['maritalStatus', 'numberOfChildren', 'serviceType', 'userNotes']].head(5)
            st.dataframe(sample_data, use_container_width=True)
    
    # Download buttons
    st.subheader("⬇️ Download Results")
    
    # Generate filenames with options
    filename_suffix = "_Spendable"
    if valid_from_date or valid_to_date:
        filename_suffix += "_WithDates"
    if remove_min_max:
        filename_suffix += "_MinMaxRemoved"
    if single_max_children or married_max_children:
        filename_suffix += "_UnlimitedChildren"
    
    col1, col2 = st.columns(2)
    
    with col1:
        if len(final_output) > 0:
            csv_final = final_output.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📊 Download Final Dataset",
                data=csv_final,
                file_name=f"Spendable_Final_Dataset{filename_suffix}.csv",
                mime="text/csv",
                type="primary"
            )
    
    with col2:
        if len(missing_output) > 0:
            csv_missing = missing_output.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="⚠️ Download Missing Locations",
                data=csv_missing,
                file_name=f"Spendable_Missing_Locations{filename_suffix}.csv",
                mime="text/csv"
            )
        else:
            st.info("No missing locations to download")

def render_cola_workflow():
    """Render the COLA workflow interface (placeholder for future development)"""
    
    st.header("🏛️ COLA Data Transformation")
    st.info("🚧 **Coming Soon**: Cost of Living Allowance transformation workflow is currently under development.")
    
    # Placeholder UI structure for COLA
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📁 File Upload (Preview)")
        st.file_uploader("Upload COLA Raw Data CSV", type=["csv"], disabled=True, key="cola_raw_data")
        st.file_uploader("Upload COLA Reference Data CSV", type=["csv"], disabled=True, key="cola_reference")
    
    with col2:
        st.subheader("⚙️ Processing Options (Preview)")
        st.checkbox("Apply Geographic Adjustments", disabled=True, key="cola_geo_adj")
        st.selectbox("COLA Calculation Method", ["Standard", "Enhanced", "Custom"], disabled=True, key="cola_method")
        st.text_input("Base Year", disabled=True, key="cola_base_year")
    
    st.markdown("---")
    st.markdown("**Planned Features:**")
    st.markdown("""
    - Geographic cost of living adjustments
    - Multi-year comparison analysis
    - Regional cost index calculations
    - Automated benchmark comparisons
    - Enhanced reporting with trend analysis
    """)

def render_housing_workflow():
    """Render the Housing workflow interface (placeholder for future development)"""
    
    st.header("🏠 Housing Data Transformation")
    st.info("🚧 **Coming Soon**: Housing allowance transformation workflow is currently under development.")
    
    # Placeholder UI structure for Housing
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📁 File Upload (Preview)")
        st.file_uploader("Upload Housing Raw Data CSV", type=["csv"], disabled=True, key="housing_raw_data")
        st.file_uploader("Upload Housing Market Data CSV", type=["csv"], disabled=True, key="housing_market")
    
    with col2:
        st.subheader("⚙️ Processing Options (Preview)")
        st.checkbox("Include Utility Allowances", disabled=True, key="housing_utilities")
        st.selectbox("Housing Type", ["Single Family", "Apartment", "Both"], disabled=True, key="housing_type")
        st.number_input("Square Footage Threshold", disabled=True, key="housing_sqft")
    
    st.markdown("---")
    st.markdown("**Planned Features:**")
    st.markdown("""
    - Housing market data integration
    - Utility allowance calculations
    - Property type categorization
    - Regional housing cost analysis
    - Seasonal adjustment factors
    """)

def render_per_diem_workflow():
    """Render the Per Diem workflow interface (placeholder for future development)"""
    
    st.header("🍽️ Per Diem Data Transformation")
    st.info("🚧 **Coming Soon**: Daily allowance transformation workflow is currently under development.")
    
    # Placeholder UI structure for Per Diem
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📁 File Upload (Preview)")
        st.file_uploader("Upload Per Diem Raw Data CSV", type=["csv"], disabled=True, key="perdiem_raw_data")
        st.file_uploader("Upload Rate Tables CSV", type=["csv"], disabled=True, key="perdiem_rates")
    
    with col2:
        st.subheader("⚙️ Processing Options (Preview)")
        st.checkbox("Apply Meal Reduction Factors", disabled=True, key="perdiem_meal_reduction")
        st.selectbox("Rate Period", ["Daily", "Weekly", "Monthly"], disabled=True, key="perdiem_period")
        st.date_input("Effective Date", disabled=True, key="perdiem_effective")
    
    st.markdown("---")
    st.markdown("**Planned Features:**")
    st.markdown("""
    - Daily meal and incidental calculations
    - Lodging allowance processing
    - Travel day adjustments
    - High-cost area supplements
    - Custom rate table management
    """)

# Sidebar with navigation information
def render_sidebar():
    """Render sidebar with application information"""
    
    st.sidebar.title("🔄 Platform Info")
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("📊 Workflow Status")
    
    # Workflow status indicators
    workflows = {
        "💰 Spendable": "✅ Active",
        "🏛️ COLA": "🚧 In Development", 
        "🏠 Housing": "🚧 In Development",
        "🍽️ Per Diem": "🚧 In Development"
    }
    
    for workflow, status in workflows.items():
        st.sidebar.markdown(f"**{workflow}**: {status}")
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("📖 Usage Instructions")
    
    with st.sidebar.expander("Spendable Workflow"):
        st.markdown("""
        **Required Files:**
        - Raw data CSV with family status columns
        - Location mapping CSV with UUID data
        
        **Features:**
        - Family status expansion (Single1-5)
        - XYNA exception handling
        - Date configuration
        - Min/max removal options
        - Unlimited children settings
        """)
    
    with st.sidebar.expander("Future Workflows"):
        st.markdown("""
        **COLA**: Cost of living adjustments
        **Housing**: Housing allowance calculations  
        **Per Diem**: Daily allowance processing
        
        These workflows will be added in future releases with specialized processing logic for each data type.
        """)

if __name__ == "__main__":
    render_sidebar()
    main()