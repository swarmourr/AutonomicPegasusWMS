# monitor_app.py
import streamlit as st
import time
import os
import sys
from monitorTiny import WorkflowDatabase, WorkflowRegister


st.write(sys.modules)
register = WorkflowRegister(db)
st.write("registred")
# Initialize TinyDB and Workflow Register
@st.cache_resource
def init_database():
    db = WorkflowDatabase("workflows.json")
    register = WorkflowRegister(db)
    return db, register

db, register = init_database()

# Streamlit Page Configuration
st.set_page_config(page_title="Workflow Monitor", page_icon="🔄", layout="wide")

# Add Title and Instructions
st.title("Workflow Monitoring System")
st.write(
    "This app monitors Pegasus workflows and their statuses in real-time. It tracks workflows and updates their status."
)

# Function to display current workflows
def display_workflows():
    st.subheader("Current Workflows")
    workflows = db.get_all_workflows()
    
    if workflows:
        # Create columns for better organization
        for wf in workflows:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write(f"**Workflow ID**: {wf['workflow_id']}")
            with col2:
                st.write(f"**Directory**: {wf['iwd']}")
            with col3:
                status = wf['status']
                if status == "Success":
                    st.success(status)
                elif status == "Failure":
                    st.error(status)
                elif status == "Running":
                    st.info(status)
                else:
                    st.warning(status)
            st.divider()
    else:
        st.info("No workflows are being monitored.")

# Add a new workflow
def add_new_workflow():
    st.subheader("Add New Workflow")
    
    with st.form("new_workflow_form"):
        workflow_id = st.text_input("Workflow ID")
        iwd = st.text_input("Directory (IWD)")
        submitted = st.form_submit_button("Start Monitoring")
        
        if submitted:
            if workflow_id and iwd:
                register.add_workflow(workflow_id, iwd)
                st.success(f"Workflow {workflow_id} is now being monitored.")
            else:
                st.warning("Please provide both Workflow ID and Directory.")

# Main app function
def main():
    # Display current workflows and their statuses
    display_workflows()
    
    # Add a new workflow to monitor
    add_new_workflow()
    
    # Add auto-refresh functionality
    if st.button("Refresh Data"):
        st.experimental_rerun()
    
    # Optional: Add auto-refresh countdown
    placeholder = st.empty()
    with placeholder.container():
        refresh_interval = 10
        for seconds in range(refresh_interval, 0, -1):
            st.write(f"Next refresh in {seconds} seconds...")
            time.sleep(1)
        placeholder.empty()
        st.experimental_rerun()

if __name__ == "__main__":
    main()