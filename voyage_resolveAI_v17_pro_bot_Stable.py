import streamlit as st
import requests
import time
import json
import pandas as pd
import plotly.express as px
import re


# === PAGE CONFIG ===
st.set_page_config(
    page_title="🧭 Voyage ResolveAI + Genie Dashboard + Chat",
    page_icon="💬",
    layout="wide"
)

# === HEADER ===
st.markdown("""
<div style="text-align:center; margin-top:-30px;">
    <h1 style="font-size:2.4em; color:#1E90FF;">🧭 Insights AI Bot (PipelineFlow)</h1>
    <p style="color:gray; font-size:1.05em;">
        Databricks Genie Integration • Structured Error Extraction • Error Trends • Interactive Chat
    </p>
    <hr style="margin-top:10px; margin-bottom:25px;">
</div>
""", unsafe_allow_html=True)

# === SIDEBAR ===
with st.sidebar:
    st.header("⚙️ Databricks Settings")
    databricks_instance = st.text_input("Workspace URL", "")
    personal_access_token = st.text_input("Personal Access Token", type="password", value="")
    space_id = st.text_input("Genie Space ID", "")
    warehouse_name = st.text_input("SQL Warehouse Name", "")
    
    # ✅ Microsoft Teams Webhook re-added
    teams_webhook_url = st.text_input("Microsoft Teams Webhook (optional)", value="")
    
    show_debug = st.checkbox("Show raw Genie JSON", value=False)
    st.markdown("---")

# === SESSION STATE ===
if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = None
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "messages" not in st.session_state:
    st.session_state.messages = []

# === HELPERS ===
def headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def ensure_warehouse_running(databricks_instance, token, warehouse_name):
    url = f"{databricks_instance}/api/2.0/sql/warehouses"
    resp = requests.get(url, headers=headers(token))
    if not resp.ok:
        st.error("Failed to fetch warehouses.")
        return False
    warehouses = resp.json().get("warehouses", [])
    wh = next((w for w in warehouses if w["name"] == warehouse_name), None)
    if not wh:
        st.error(f"Warehouse '{warehouse_name}' not found.")
        return False
    if wh["state"] == "RUNNING":
        st.success("✅ Warehouse already running.")
        return True
    st.info(f"⏳ Starting warehouse {warehouse_name}...")
    requests.post(f"{databricks_instance}/api/2.0/sql/warehouses/{wh['id']}/start", headers=headers(token))
    for _ in range(60):
        time.sleep(3)
        check = requests.get(f"{databricks_instance}/api/2.0/sql/warehouses/{wh['id']}", headers=headers(token))
        if check.json().get("state") == "RUNNING":
            st.success("✅ Warehouse is now running.")
            return True
    st.warning("⚠️ Timeout waiting for warehouse to start.")
    return False

def call_genie(databricks_instance, token, space_id, warehouse_name, prompt):
    start_url = f"{databricks_instance}/api/2.0/genie/spaces/{space_id}/start-conversation"
    payload = {"content": prompt, "data_source_name": warehouse_name}
    resp = requests.post(start_url, headers=headers(token), json=payload)
    if not resp.ok:
        raise Exception(f"Failed to start conversation: {resp.text}")
    conv_id = resp.json()["conversation"]["id"]
    messages_url = f"{databricks_instance}/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages"
    for _ in range(150):
        time.sleep(2)
        r = requests.get(messages_url, headers=headers(token))
        msgs = r.json().get("messages", [])
        if not msgs:
            continue
        last = msgs[-1]
        status = last.get("status", "")
        if status == "COMPLETED":
            attachments = last.get("attachments", [])
            if attachments:
                att_id = attachments[0].get("attachment_id")
                query_url = (
                    f"{databricks_instance}/api/2.0/genie/spaces/{space_id}/conversations/"
                    f"{conv_id}/messages/{last['message_id']}/query-result/{att_id}"
                )
                result = requests.get(query_url, headers=headers(token)).json()
                rows = result.get("statement_response", {}).get("result", {}).get("data_array", [])
                return rows, result
            else:
                return [], r.json()
        elif status in ("FAILED", "CANCELLED"):
            raise Exception("⚠️ Genie failed to generate response.")
    raise TimeoutError("⏰ Genie did not complete in time.")

# ✅ Teams integration helper
def send_to_teams(webhook_url: str, message: str):
    """Send message to a Microsoft Teams channel via webhook."""
    if not webhook_url or not webhook_url.strip():
        return
    try:
        resp = requests.post(webhook_url, json={"text": message})
        if resp.status_code == 200:
            st.success("📤 Message sent to Microsoft Teams.")
        else:
            st.warning(f"⚠️ Failed to send to Teams (HTTP {resp.status_code}).")
    except Exception as e:
        st.error(f"❌ Error sending message to Teams: {e}")

# === RENDER BOXED ERROR REPORT ===
# === RENDER BOXED ERROR REPORT ===
def render_genie_pipeline_errors(rows):
    if not rows:
        st.info("✅ No recent failures found.")
        return

    st.markdown("## 🚨 Genie Pipeline Errors Report")

    # Databricks URL pattern (http/https + hostname + /?o=... or #job/... etc.)
    url_regex = re.compile(
        r'https?://[a-zA-Z0-9\.\-]+\.azuredatabricks\.net[^\s)"]+',
        flags=re.IGNORECASE,
    )

    for i, row in enumerate(rows, start=1):
        try:
            pipeline_name = row[0] if len(row) > 0 else "Unknown Pipeline"
            details_json = row[1] if len(row) > 1 else "{}"
            timestamp = row[2] if len(row) > 2 else "N/A"

            try:
                details = json.loads(details_json)
            except Exception:
                details = {}

            # Extract details
            data_factory = details.get("DataFactoryName", "az22q1-O2CIA-adf")
            run_id = details.get("RunID") or details.get("runId") or "Not Available"
            trigger_time = details.get("TriggerTime") or details.get("trigger_time") or timestamp
            region = details.get("Region", "NA,AP,EU")
            gold_table = details.get("GoldTableName", "Not Available")
            message = (
                details.get("Message")
                or details.get("error_message")
                or "No error message available."
            )
            run_url = details.get("runPageUrl") or details.get("RunPageUrl") or ""
            project_name = details.get("ProjectName") or "Others"
            status = details.get("Status") or details.get("status") or "Failure"

            # If run_url not provided, try to extract from the error message
            if not run_url:
                m = url_regex.search(message or "")
                if m:
                    run_url = m.group(0)

            # Make a clickable link (or show "Not Available")
            run_url_display = "Not Available"
            if isinstance(run_url, str) and run_url.startswith("http"):
                run_url_display = f"<a href='{run_url}' target='_blank' rel='noopener noreferrer'>{run_url}</a>"

            # Render a neat card
            st.markdown(
                f"""
<div style="background-color:#f9f9f9; border:1px solid #e5e7eb; border-radius:12px; padding:18px; margin:14px 0; box-shadow:0 1px 2px rgba(0,0,0,0.03);">
  <div style="font-weight:700; font-size:1.05rem; margin-bottom:10px;">🚨 Error {i}</div>

  <div><b>DataFactory Name:</b> {data_factory}</div>
  <div><b>Pipeline Name:</b> {pipeline_name}</div>
  <div><b>Run ID:</b> {run_id}</div>
  <div><b>Trigger Time:</b> {trigger_time}</div>
  <div><b>Region:</b> {region}</div>
  <div><b>GoldTableName:</b> {gold_table}</div>
  <div style="margin-top:6px;"><b>Error Message:</b> {message}</div>

  <div style="margin-top:6px;"><b>Run URL:</b> {run_url_display}</div>
  <div><b>ProjectName:</b> {project_name}</div>
  <div><b>Status:</b> {status}</div>
</div>
""",
                unsafe_allow_html=True,
            )

        except Exception as e:
            st.error(f"⚠️ Error displaying entry {i}: {e}")


# === FETCH LATEST ERRORS ===
st.markdown("### 💡 Select Error Fetching Options")
num_errors = st.number_input("Number of recent errors (1–10)", min_value=1, max_value=10, value=3)
fetch_btn = st.button("🔍 Fetch Latest Errors")

if fetch_btn:
    try:
        st.info(f"⏳ Checking warehouse {warehouse_name} status...")
        if ensure_warehouse_running(databricks_instance, personal_access_token, warehouse_name):
            prompt = (
                f"Give me the list of distinct {num_errors} error messages in last 48 Hours "
                "list it in the tabular format with the pipeline names, the error message (from the properties column), "
                "and the timestamp column and the projectName. The table name ends with '_incremental'."
            )
            st.write("😃 Sending prompt to Genie...")
            rows, raw = call_genie(databricks_instance, personal_access_token, space_id, warehouse_name, prompt)
            render_genie_pipeline_errors(rows)
            if teams_webhook_url:
                count = len(rows) if rows else 0
                alert_message = f"🚨 ResolveAI Alert: {count} pipeline error(s) detected.\nTriggered from Genie dashboard."
                send_to_teams(teams_webhook_url, alert_message)
            if show_debug:
                with st.expander("🔍 Raw Genie Response"):
                    st.json(raw)
    except Exception as e:
        st.error(f"⚠️ Error while fetching latest errors: {e}")

# === 📈 ERROR TRENDS DASHBOARD ===
# === 📈 ERROR TRENDS DASHBOARD ===
with st.expander("📊 View Error Trends Dashboard (Unique Failures × Day)", expanded=False):
    if st.button("📈 Run Error Trend Query", use_container_width=True):
        try:
            ensure_warehouse_running(databricks_instance, personal_access_token, warehouse_name)

            prompt = """
            Show a daily trend of all unique failures from the pipeline_runs_incremental_genie table
            where the properties column contains "Message_Failure" or "failure".
            Count each pipeline only once per day, remove duplicates, group by timestamp (day),
            and display both a summary table and a line chart of total unique failure counts by day.
            """

            rows, result = call_genie(databricks_instance, personal_access_token, space_id, warehouse_name, prompt)
            data = result.get("statement_response", {}).get("result", {}).get("data_array", [])
            schema = result.get("statement_response", {}).get("manifest", {}).get("schema", {}).get("columns", [])

            if not data or not schema:
                st.warning("⚠️ No data returned for error trends.")
            else:
                cols = [c["name"] for c in schema]  # keep original case for display
                df = pd.DataFrame(data, columns=cols)

                # Show raw table
                st.dataframe(df, use_container_width=True)

                # -------- Robust column detection --------
                lower_map = {c.lower(): c for c in df.columns}  # map lower->original

                # Prefer these for the date column (first match wins)
                date_priority = ["day", "date", "failure_day", "failure_date", "timestamp"]
                date_col = None
                for key in date_priority:
                    if key in lower_map:
                        date_col = lower_map[key]
                        break
                if date_col is None:
                    # fallback: any column containing day/date/timestamp
                    for c in df.columns:
                        lc = c.lower()
                        if any(k in lc for k in ["day", "date", "timestamp"]):
                            date_col = c
                            break

                # Prefer these for the count column (first match wins)
                count_priority = [
                    "unique_failure_count",
                    "unique_failed_pipeline_count",
                    "pipeline_failed_count",
                    "failure_count",
                    "count"
                ]
                count_col = None
                for key in count_priority:
                    if key in lower_map:
                        count_col = lower_map[key]
                        break
                if count_col is None:
                    # fallback: any column containing 'count' BUT NOT date-like columns
                    for c in df.columns:
                        lc = c.lower()
                        if "count" in lc and not any(k in lc for k in ["day", "date", "timestamp"]):
                            count_col = c
                            break

                if not date_col or not count_col:
                    st.warning(
                        "⚠️ Could not identify columns for date and unique failure count. "
                        "Expected something like [day/date/timestamp] and [*_count]."
                    )
                else:
                    # Coerce types
                    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
                    df[count_col] = pd.to_numeric(df[count_col], errors="coerce")

                    # Drop rows with missing x or y, sort by date
                    df = df.dropna(subset=[date_col, count_col]).sort_values(by=date_col)

                    # Plot
                    fig = px.line(
                        df,
                        x=date_col,
                        y=count_col,
                        markers=True,
                        title="📈 Unique Failures per Day (Deduplicated)",
                        labels={date_col: "Day", count_col: "Unique Failure Count"},
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    fig.update_traces(line=dict(width=3))
                    fig.update_xaxes(tickformat="%Y-%m-%d")
                    fig.update_layout(yaxis=dict(tickformat="~s"))  # clean numbers (no scientific)
                    st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"❌ Error: {e}")

# === 📊 DAILY FAILURE SUMMARY BY TYPE DASHBOARD ===
# === 📊 DAILY FAILURE SUMMARY BY TYPE DASHBOARD ===


# === 📊 DAILY FAILURE SUMMARY BY TYPE DASHBOARD ===
# === 📊 DAILY FAILURE SUMMARY (STACKED BAR CHART) ===
with st.expander("📊 View Daily Failure Summary by Type (Stacked by Day)", expanded=False):
    if st.button("🚀 Run Stacked Daily Failure Summary Query", use_container_width=True):
        try:
            ensure_warehouse_running(databricks_instance, personal_access_token, warehouse_name)

            prompt = """
            From the pipeline_runs_incremental_genie table, show a daily summary of unique failures where the properties column contains "Message_Failure" or "failure".
            Count each pipeline only once per day (remove duplicates), and classify failures from the text in Message_Failure into categories:
            - Access_Notebook_Failure (text like “unable to access the notebook”)
            - Workload_Failure (text like “workload failed”)
            - Unexpected_Failure (text like “unexpected failure”)
            - Invalid_Access_Token (text like “Invalid Access Token”)
            - Others (all remaining failure messages).
            Return one row per day with these columns in order:
            Day,
            Unique_Failure_Count (total unique failures for that day),
            Access_Notebook_Failure,
            Workload_Failure,
            Unexpected_Failure,
            Invalid_Access_Token,
            Others.
            Then visualize the result as a stacked bar chart with Day on the X-axis and failure categories stacked by color.
            """

            # === GENIE CALL ===
            rows, result = call_genie(databricks_instance, personal_access_token, space_id, warehouse_name, prompt)
            data = result.get("statement_response", {}).get("result", {}).get("data_array", [])
            schema = result.get("statement_response", {}).get("manifest", {}).get("schema", {}).get("columns", [])

            if not data or not schema:
                st.warning("⚠️ No data returned for Daily Failure Summary query.")
            else:
                cols = [c["name"].strip() for c in schema] if schema else [str(i) for i in range(len(data[0]))]
                df = pd.DataFrame(data, columns=cols)
                st.dataframe(df, use_container_width=True)

                # Detect the date column
                date_col = next((c for c in df.columns if any(k in c.lower() for k in ["day", "date", "timestamp"])), None)

                if date_col:
                    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

                    # Define expected order for columns
                    expected_order = [
                        "Unique_Failure_Count",
                        "Access_Notebook_Failure",
                        "Workload_Failure",
                        "Unexpected_Failure",
                        "Invalid_Access_Token",
                        "Others"
                    ]
                    ordered_cols = [c for c in expected_order if c in df.columns]

                    # === Transform Data for Stacked Visualization ===
                    df_melted = df.melt(
                        id_vars=[date_col],
                        value_vars=ordered_cols,
                        var_name="Failure_Type",
                        value_name="Count"
                    )

                    # === Proper Stacked Bar Chart ===
                    fig_stacked = px.bar(
                        df_melted,
                        x=date_col,
                        y="Count",
                        color="Failure_Type",
                        barmode="stack",  # ✅ Stacked, not grouped
                        text="Count",
                        title="📊 Daily Unique Failure Categories (Stacked Bar Chart by Day)",
                        labels={"Count": "Failure Count", date_col: "Day"},
                        color_discrete_sequence=px.colors.qualitative.Vivid
                    )

                    fig_stacked.update_traces(
                        textposition="inside",
                        textfont=dict(size=10)
                    )
                    fig_stacked.update_layout(
                        xaxis_title="Day",
                        yaxis_title="Failure Count",
                        bargap=0.25,
                        bargroupgap=0.05,
                        legend_title="Failure Type",
                        template="plotly_white",
                        height=550
                    )
                    fig_stacked.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_stacked, use_container_width=True)

                    # === Optional Line Chart for Total Unique Failures ===
                    if "Unique_Failure_Count" in df.columns:
                        fig_line = px.line(
                            df,
                            x=date_col,
                            y="Unique_Failure_Count",
                            markers=True,
                            title="📈 Total Unique Failures per Day (Deduplicated)",
                            color_discrete_sequence=["#1f77b4"]
                        )
                        fig_line.update_traces(line=dict(width=3))
                        fig_line.update_xaxes(tickformat="%Y-%m-%d")
                        st.plotly_chart(fig_line, use_container_width=True)

                else:
                    st.warning("⚠️ Could not detect a Day or Date column for charting.")

        except Exception as e:
            st.error(f"❌ Error while fetching Stacked Daily Failure Summary: {e}")


# === 🧩 BLUEYONDER ERROR RCA DASHBOARD ===
# === 🧠 BLUEYONDER RCA DASHBOARD (LAST 240 HOURS) ===
with st.expander("🧩 View BlueYonder RCA Summary (Last 240 Hours)", expanded=False):
    if st.button("🚀 Run BlueYonder RCA Query", use_container_width=True):
        try:
            ensure_warehouse_running(databricks_instance, personal_access_token, warehouse_name)

            num_errors = st.session_state.get("num_errors", 10) if "num_errors" in st.session_state else 10

            prompt = f"""
            From the pipeline_runs_incremental_genie table, retrieve the distinct {num_errors} most recent pipeline
            failure records from the last 240 hours for BlueYonder-related projects.
            Consider project names matching 'BlueYonder', 'blueyonder', or 'Blue Yonder'.
            Include rows even if the properties column is partially null.

            Extract and display the following columns:
              • PipelineName
              • ProjectName
              • Failure_Type (derived from message text)
              • Error_Message (from properties column or Message_Failure)
              • RCA (if available)
              • Timestamp (from the timestamp column)

            Classify Failure_Type as:
              - Access_Notebook_Failure → if message contains 'unable to access the notebook'
              - Workload_Failure → if message contains 'workload failed'
              - Unexpected_Failure → if message contains 'unexpected failure'
              - Invalid_Access_Token → if message contains 'Invalid Access Token'
              - Others → otherwise

            Remove duplicate rows (unique by PipelineName + Error_Message).
            Sort results by timestamp in descending order.
            Output as a formatted table.
            """

            # === GENIE CALL ===
            rows, result = call_genie(
                databricks_instance,
                personal_access_token,
                space_id,
                warehouse_name,
                prompt
            )

            data = result.get("statement_response", {}).get("result", {}).get("data_array", [])
            schema = result.get("statement_response", {}).get("manifest", {}).get("schema", {}).get("columns", [])

            if not data or not schema:
                st.warning("⚠️ No BlueYonder RCA data found for the last 240 hours (case-insensitive filter applied).")
            else:
                cols = [c["name"].strip() for c in schema]
                df = pd.DataFrame(data, columns=cols)
                df.columns = [col.replace("_", " ").title() for col in df.columns]

                # Convert timestamp
                ts_col = next((c for c in df.columns if "timestamp" in c.lower() or "date" in c.lower()), None)
                if ts_col:
                    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")

                # Display table
                st.markdown("### 📋 BlueYonder Failures (Last 240 Hours)")
                st.dataframe(df, use_container_width=True)

                # Optional chart
                if ts_col and "Failure Type" in df.columns:
                    summary = df.groupby(["Failure Type"]).size().reset_index(name="Count")
                    fig = px.bar(
                        summary,
                        x="Failure Type",
                        y="Count",
                        color="Failure Type",
                        title="📊 BlueYonder Failures by Type (Last 240 Hours)",
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"❌ Error fetching BlueYonder RCA data: {e}")




# === 💬 CHAT / CONVERSATION SECTION ===
# === 💬 CHAT / CONVERSATION SECTION ===
# === 💬 CHAT / CONVERSATION SECTION ===
st.markdown("## 💬 Chat with Databricks Genie")

# Initialize last dataframe storage
if "last_df" not in st.session_state:
    st.session_state.last_df = None

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- Chat input ---
if prompt := st.chat_input("Ask Genie anything about your Databricks data..."):
    st.chat_message("user").markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    try:
        ensure_warehouse_running(databricks_instance, personal_access_token, warehouse_name)
        rows, res = call_genie(databricks_instance, personal_access_token, space_id, warehouse_name, prompt)

        # --- If Genie returns new rows (structured data) ---
        if rows:
            st.chat_message("assistant").markdown("📊 Here's the data Genie found:")
            df = pd.DataFrame(rows)
            st.session_state.last_df = df.copy()  # Store for reuse
            st.dataframe(df, use_container_width=True)

            # Normalize columns
            df.columns = [str(c).lower() for c in df.columns]
            if "error_day" not in df.columns:
                if 0 in df.columns or "0" in df.columns:
                    df.rename(columns={0: "error_day", "0": "error_day"}, inplace=True)
            if "failuretype" not in df.columns:
                if 1 in df.columns or "1" in df.columns:
                    df.rename(columns={1: "failuretype", "1": "failuretype"}, inplace=True)
            if "error_count" not in df.columns:
                if 2 in df.columns or "2" in df.columns or "count" in df.columns:
                    df.rename(columns={2: "error_count", "2": "error_count", "count": "error_count"}, inplace=True)

            # Detect if user requested chart
            show_chart = any(
                word in prompt.lower()
                for word in ["chart", "graph", "plot", "visualization", "trend", "bar", "line", "pie"]
            )

            if show_chart and all(col in df.columns for col in ["error_day", "failuretype", "error_count"]):
                df["error_day"] = pd.to_datetime(df["error_day"], errors="coerce")
                df["error_count"] = pd.to_numeric(df["error_count"], errors="coerce")

                # Choose chart type
                chart_type = "line"
                if "bar" in prompt.lower():
                    chart_type = "bar"
                elif "pie" in prompt.lower():
                    chart_type = "pie"
                elif "line" in prompt.lower():
                    chart_type = "line"
                elif "trend" in prompt.lower():
                    chart_type = "line"

                # --- Line chart ---
                if chart_type == "line":
                    fig = px.line(
                        df,
                        x="error_day",
                        y="error_count",
                        color="failuretype",
                        markers=True,
                        title="📈 Error Counts by Type and Day (Line Chart)",
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    fig.update_traces(line=dict(width=3))
                    st.plotly_chart(fig, use_container_width=True)

                # --- Bar chart ---
                elif chart_type == "bar":
                    fig = px.bar(
                        df,
                        x="error_day",
                        y="error_count",
                        color="failuretype",
                        title="📊 Grouped Error Counts by Day (Bar Chart)",
                        barmode="group",
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # --- Pie chart ---
                elif chart_type == "pie":
                    fig = px.pie(
                        df,
                        names="failuretype",
                        values="error_count",
                        title="🥧 Error Distribution by Type (Pie Chart)",
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    st.plotly_chart(fig, use_container_width=True)

            else:
                st.info("✅ Data displayed. Visualization skipped because prompt did not request a chart.")

            reply_text = f"✅ Genie retrieved {len(rows)} records."
            st.chat_message("assistant").markdown(reply_text)
            st.session_state.messages.append({"role": "assistant", "content": reply_text})

        # --- If Genie returns no new rows but user asks for chart ---
        else:
            chart_keywords = ["chart", "graph", "plot", "trend", "visualization", "bar", "line", "pie"]
            if any(k in prompt.lower() for k in chart_keywords) and st.session_state.last_df is not None:
                df = st.session_state.last_df.copy()
                st.chat_message("assistant").markdown("📊 Reusing last dataset for visualization...")

                df.columns = [str(c).lower() for c in df.columns]
                if "error_day" not in df.columns:
                    if 0 in df.columns or "0" in df.columns:
                        df.rename(columns={0: "error_day", "0": "error_day"}, inplace=True)
                if "failuretype" not in df.columns:
                    if 1 in df.columns or "1" in df.columns:
                        df.rename(columns={1: "failuretype", "1": "failuretype"}, inplace=True)
                if "error_count" not in df.columns:
                    if 2 in df.columns or "2" in df.columns or "count" in df.columns:
                        df.rename(columns={2: "error_count", "2": "error_count", "count": "error_count"}, inplace=True)

                if all(col in df.columns for col in ["error_day", "failuretype", "error_count"]):
                    df["error_day"] = pd.to_datetime(df["error_day"], errors="coerce")
                    df["error_count"] = pd.to_numeric(df["error_count"], errors="coerce")

                    chart_type = "line"
                    if "bar" in prompt.lower():
                        chart_type = "bar"
                    elif "pie" in prompt.lower():
                        chart_type = "pie"
                    elif "line" in prompt.lower():
                        chart_type = "line"
                    elif "trend" in prompt.lower():
                        chart_type = "line"

                    # --- Line chart ---
                    if chart_type == "line":
                        fig = px.line(
                            df,
                            x="error_day",
                            y="error_count",
                            color="failuretype",
                            markers=True,
                            title="📈 Error Counts by Type and Day (Line Chart)",
                            color_discrete_sequence=px.colors.qualitative.Set2
                        )
                        fig.update_traces(line=dict(width=3))
                        st.plotly_chart(fig, use_container_width=True)

                    # --- Bar chart ---
                    elif chart_type == "bar":
                        fig = px.bar(
                            df,
                            x="error_day",
                            y="error_count",
                            color="failuretype",
                            title="📊 Grouped Error Counts by Day (Bar Chart)",
                            barmode="group",
                            color_discrete_sequence=px.colors.qualitative.Set2
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    # --- Pie chart ---
                    elif chart_type == "pie":
                        fig = px.pie(
                            df,
                            names="failuretype",
                            values="error_count",
                            title="🥧 Error Distribution by Type (Pie Chart)",
                            color_discrete_sequence=px.colors.qualitative.Set2
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    st.session_state.messages.append({"role": "assistant", "content": f"📊 Displayed {chart_type} chart using previous data."})
                else:
                    st.info("⚠️ Data available but missing required columns for visualization.")
            else:
                st.chat_message("assistant").markdown("✅ Genie processed your request.")
                st.session_state.messages.append({"role": "assistant", "content": "✅ Genie processed your request."})

        # Debug output
        if show_debug:
            st.json(res)

        # Optional: Microsoft Teams webhook notification
        if teams_webhook_url:
            try:
                summary_message = {
                    "text": f"📢 *Databricks Genie Update*\n\n**Prompt:** {prompt}\n**Result:** {len(rows)} records retrieved."
                }
                requests.post(teams_webhook_url, json=summary_message)
            except Exception as e:
                st.warning(f"⚠️ Failed to send message to Teams: {e}")

    except Exception as e:
        st.error(f"❌ Error during conversation: {e}")

# === FOOTER ===
st.markdown("""
<hr>
<div style="text-align:center; color:gray; font-size:0.9em;">
Voyage Issue Resolution Bot • Genie v15 • Dashboard + Chat + Teams Integration • © 2025
</div>
""", unsafe_allow_html=True)
