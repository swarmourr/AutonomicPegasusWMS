import streamlit as st
import json
import os
import pandas as pd

# Page configuration
st.set_page_config(
    page_title="Pegasus Workflow Monitor",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for styling
st.markdown("""
    <style>
        .css-1aumxhk {
            background-color: #F7F7F9;
        }
        .title-text {
            font-size: 2rem;
            color: #4CAF50;
            font-weight: bold;
        }
        .section-title {
            font-size: 1.5rem;
            color: #2196F3;
            margin-top: 20px;
        }
        .status-success {
            color: #4CAF50;
            font-weight: bold;
        }
        .status-failure {
            color: #F44336;
            font-weight: bold;
        }
        .status-monitoring {
            color: #FF9800;
            font-weight: bold;
        }
    </style>
""", unsafe_allow_html=True)

# Header
st.markdown("<div class='title-text'>Pegasus Workflow Monitoring Dashboard</div>", unsafe_allow_html=True)

# Sidebar
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to:", ["Monitoring", "Historical Data", "Help"])

# Constants
LOGS_DIR = "logs"

# Monitoring Workflows
if page == "Monitoring":
    st.markdown("<div class='section-title'>Active Workflow Monitoring</div>", unsafe_allow_html=True)
    monitored_workflows = []
    
    # Check logs for active workflows
    for folder in os.listdir(LOGS_DIR):
        if os.path.isdir(os.path.join(LOGS_DIR, folder)):
            monitored_workflows.append(folder)

    if monitored_workflows:
        selected_workflow = st.selectbox("Select a workflow:", monitored_workflows)

        if selected_workflow:
            st.write(f"### Details for Workflow: `{selected_workflow}`")

            # Display logs
            log_file = os.path.join(LOGS_DIR, selected_workflow, f"{selected_workflow}_monitor.log")
            if os.path.exists(log_file):
                with open(log_file, "r") as f:
                    st.text_area("Log Output", f.read(), height=300)

            # Display held jobs
            held_jobs_file = os.path.join(LOGS_DIR, selected_workflow, f"{selected_workflow}_held_jobs.json")
            if os.path.exists(held_jobs_file):
                with open(held_jobs_file, "r") as f:
                    data = json.load(f)
                held_jobs_df = pd.DataFrame(data.get("held_jobs", []))
                if not held_jobs_df.empty:
                    st.markdown("### Held Jobs")
                    st.dataframe(held_jobs_df)
                else:
                    st.success("No held jobs detected.")
            else:
                st.info("No held jobs file found.")
    else:
        st.warning("No active workflows are being monitored.")

# Historical Data
elif page == "Historical Data":
    st.markdown("<div class='section-title'>Workflow Historical Data</div>", unsafe_allow_html=True)
    workflows_summary = []

    # Summarize workflow statuses from logs
    for folder in os.listdir(LOGS_DIR):
        if os.path.isdir(os.path.join(LOGS_DIR, folder)):
            log_file = os.path.join(LOGS_DIR, folder, f"{folder}_monitor.log")
            if os.path.exists(log_file):
                workflows_summary.append({
                    "Workflow ID": folder,
                    "Status": "Completed" if "Workflow is completed" in open(log_file).read() else "In Progress"
                })

    # Display as a table
    if workflows_summary:
        df = pd.DataFrame(workflows_summary)
        st.table(df)
    else:
        st.info("No historical data available.")

# Help Section
elif page == "Help":
    st.markdown("<div class='section-title'>Help & Usage Instructions</div>", unsafe_allow_html=True)
    st.write("""
    - **Monitoring:** Displays the status of active workflows being monitored.
    - **Historical Data:** Provides an overview of completed or failed workflows.
    - **Logs:** Check individual workflow logs for detailed information.
    
    ### Troubleshooting
    - Ensure the `logs` directory is accessible and contains monitoring information.
    - For any issues, please contact support or refer to the documentation.
    """)
